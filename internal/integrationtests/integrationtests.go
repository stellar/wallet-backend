package integrationtests

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/stellar/go/amount"
	"github.com/stellar/go/hash"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/pkg/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

const txTimeout = 60 * time.Second

type IntegrationTestsOptions struct {
	BaseFee                            int64
	NetworkPassphrase                  string
	RPCService                         services.RPCService
	SourceAccountKP                    *keypair.Full
	WBClient                           *wbclient.Client
	DBConnectionPool                   db.ConnectionPool
	DistributionAccountSignatureClient signing.SignatureClient
}

func (o *IntegrationTestsOptions) Validate() error {
	rules := []struct {
		condition bool
		err       error
	}{
		{o.BaseFee < int64(txnbuild.MinBaseFee), errors.New("base fee is lower than the minimum network fee")},
		{o.NetworkPassphrase == "", errors.New("network passphrase cannot be empty")},
		{o.RPCService == nil, errors.New("rpc client cannot be nil")},
		{o.SourceAccountKP == nil, errors.New("source account keypair cannot be nil")},
		{o.WBClient == nil, errors.New("wallet backend client cannot be nil")},
		{o.DBConnectionPool == nil, errors.New("db connection pool cannot be nil")},
		{o.DistributionAccountSignatureClient == nil, errors.New("distribution account signature client cannot be nil")},
	}

	for _, rule := range rules {
		if rule.condition {
			return rule.err
		}
	}

	return nil
}

type IntegrationTests struct {
	BaseFee                            int64
	NetworkPassphrase                  string
	RPCService                         services.RPCService
	SourceAccountKP                    *keypair.Full
	WBClient                           *wbclient.Client
	ChannelAccountStore                store.ChannelAccountStore
	DBConnectionPool                   db.ConnectionPool
	DistributionAccountSignatureClient signing.SignatureClient
	Fixtures                           Fixtures
}

func (it *IntegrationTests) Run(ctx context.Context) error {
	log.Ctx(ctx).Info("🆕 Starting integration tests...")

	// Step 1: Prepare transactions locally
	fmt.Println("")
	log.Ctx(ctx).Info("===> 1️⃣ [Local] Building transactions...")
	classicOps, err := it.Fixtures.prepareClassicOps()
	if err != nil {
		return fmt.Errorf("preparing classic ops: %w", err)
	}
	buildTxRequest := types.BuildTransactionsRequest{
		Transactions: []types.Transaction{{TimeBounds: int64(txTimeout.Seconds()), Operations: classicOps}},
	}

	invokeContractOp, simResultXDR, err := it.prepareInvokeContractOp()
	if err != nil {
		return fmt.Errorf("preparing invoke contract ops: %w", err)
	}
	log.Ctx(ctx).Warnf("✅ invokeContractOp: %+v", invokeContractOp)
	log.Ctx(ctx).Warnf("✅ simResultXDR: %+v", simResultXDR)
	// Step 2: call /tss/transactions/build
	fmt.Println("")
	log.Ctx(ctx).Info("===> 2️⃣ [WalletBackend] Building transactions...")
	builtTxResponse, err := it.WBClient.BuildTransactions(ctx, buildTxRequest.Transactions...)
	if err != nil {
		return fmt.Errorf("calling buildTransactions: %w", err)
	}
	log.Ctx(ctx).Infof("✅ builtTxResponse: %+v", builtTxResponse)
	for i, txXDR := range builtTxResponse.TransactionXDRs {
		txString, innerErr := txString(txXDR)
		if innerErr != nil {
			return fmt.Errorf("building transaction string: %w", innerErr)
		}
		log.Ctx(ctx).Infof("builtTx[%d]: %s", i, txString)
	}
	it.assertBuildTransactionResult(ctx, buildTxRequest, *builtTxResponse)

	// Step 3: sign transactions with the SourceAccountKP
	fmt.Println("")
	log.Ctx(ctx).Info("===> 3️⃣ [Local] Signing transactions...")
	signedTxXDRs := make([]string, len(builtTxResponse.TransactionXDRs))
	for i, txXDR := range builtTxResponse.TransactionXDRs {
		tx, innerErr := parseTxXDR(txXDR)
		if innerErr != nil {
			return fmt.Errorf("parsing transaction from XDR: %w", innerErr)
		}
		innerTxHash, innerErr := tx.HashHex(it.NetworkPassphrase)
		if innerErr != nil {
			return fmt.Errorf("hashing transaction: %w", innerErr)
		}
		log.Ctx(ctx).Infof("=====> innerHash: %s", innerTxHash)
		signedTx, innerErr := tx.Sign(it.NetworkPassphrase, it.SourceAccountKP)
		if innerErr != nil {
			return fmt.Errorf("signing transaction: %w", innerErr)
		}
		signedTxXDR, innerErr := signedTx.Base64()
		if innerErr != nil {
			return fmt.Errorf("encoding transaction to base64: %w", innerErr)
		}
		signedTxXDRs[i] = signedTxXDR
	}

	// Step 4: call /tx/create-fee-bump for each transaction
	fmt.Println("")
	log.Ctx(ctx).Info("===> 4️⃣ [WalletBackend] Creating fee bump transaction...")
	feeBumpedTxs := make([]string, len(signedTxXDRs))
	for i, txXDR := range signedTxXDRs {
		feeBumpTxResponse, innerErr := it.WBClient.FeeBumpTransaction(ctx, txXDR)
		if innerErr != nil {
			return fmt.Errorf("calling feeBumpTransaction: %w", innerErr)
		}
		log.Ctx(ctx).Infof("✅ feeBumpTxResponse[%d]: %+v", i, feeBumpTxResponse)
		feeBumpedTxs[i] = feeBumpTxResponse.Transaction

		txString, innerErr := txString(feeBumpedTxs[i])
		if innerErr != nil {
			return fmt.Errorf("building transaction string: %w", innerErr)
		}
		log.Ctx(ctx).Infof("feeBumpedTx[%d]: %s", i, txString)

		it.assertFeeBumpTransactionResult(ctx, types.CreateFeeBumpTransactionRequest{Transaction: txXDR}, *feeBumpTxResponse)
	}

	// Step 5: wait for RPC to be healthy
	fmt.Println("")
	log.Ctx(ctx).Info("===> 5️⃣ [RPC] Waiting for RPC service to become healthy...")
	err = WaitForRPCHealthAndRun(ctx, it.RPCService, 40*time.Second, nil)
	if err != nil {
		return fmt.Errorf("waiting for RPC service to become healthy: %w", err)
	}

	// Step 6: submit transactions to RPC
	fmt.Println("")
	log.Ctx(ctx).Info("===> 6️⃣ [RPC] Submitting transactions...")
	hashes := make([]string, len(feeBumpedTxs))
	for i, txXDR := range feeBumpedTxs {
		var res entities.RPCSendTransactionResult
		res, err = it.RPCService.SendTransaction(txXDR)
		if err != nil {
			return fmt.Errorf("sending transaction %d: %w", i, err)
		}
		log.Ctx(ctx).Infof("✅ submittedTx[%d]: %+v", i, res)
		if res.Status != entities.PendingStatus {
			return fmt.Errorf("transaction %d failed with status %s and errorResultXdr %s", i, res.Status, res.ErrorResultXDR)
		}
		hashes[i] = res.Hash
	}

	// Step 7: poll the network for the transaction
	fmt.Println("")
	log.Ctx(ctx).Info("===> 7️⃣ [RPC] Waiting for transaction confirmation...")
	const retryDelay = 6 * time.Second
	for _, hash := range hashes {
		if err = WaitForTransactionConfirmation(ctx, it.RPCService, hash, retry.Delay(retryDelay), retry.Attempts(uint(txTimeout/retryDelay))); err != nil {
			return fmt.Errorf("waiting for transaction confirmation: %w", err)
		}
		log.Ctx(ctx).Infof("✅ transaction %s confirmed on Stellar network", hash)
	}

	// TODO: verifyTxResult in wallet-backend

	return nil
}

// assertBuildTransactionResult asserts that the build transaction result is correct.
func (it *IntegrationTests) assertBuildTransactionResult(ctx context.Context, req types.BuildTransactionsRequest, resp types.BuildTransactionsResponse) {
	assertOrFail(len(req.Transactions) == len(resp.TransactionXDRs), "number of transactions in request and response must be the same")

	for i, respTxXDR := range resp.TransactionXDRs {
		// Parse the transaction from the XDR
		tx, err := parseTxXDR(respTxXDR)
		assertOrFail(err == nil, "parsing transaction from XDR: %v", err)

		// Assert that the tx source account is a channel account
		txSourceAccount := tx.SourceAccount()
		channelAccount, err := it.ChannelAccountStore.Get(ctx, it.DBConnectionPool, txSourceAccount.GetAccountID())
		assertOrFail(err == nil, "error getting channel account: %v", err)
		assertOrFail(channelAccount != nil, "channel account not found in the database")
		// Assert that the tx is signed by the channel account
		assertOrFail(len(tx.Signatures()) > 0, "transaction should be signed")
		assertOrFail(tx.Signatures()[0].Hint == keypair.MustParse(channelAccount.PublicKey).Hint(), "signature at index 0 should be made by the channel account public key")

		// Assert the operations are the same
		responseOps := tx.Operations()
		requestOpsXDRs := req.Transactions[i].Operations
		assertOrFail(len(responseOps) == len(requestOpsXDRs), "number of operations in request and response must be the same")
		for j, requestOpXDRStr := range requestOpsXDRs {
			requestOpXDR, err := utils.OperationXDRFromBase64(requestOpXDRStr)
			assertOrFail(err == nil, "error converting operation string to XDR: %v", err)
			requestOp, err := utils.OperationXDRToTxnBuildOp(requestOpXDR)
			assertOrFail(err == nil, "error converting operation XDR to txnbuild operation: %v", err)
			assertOrFail(reflect.DeepEqual(requestOp, responseOps[j]), "operation %d in request and response must be the same", j)
		}
	}
}

// assertFeeBumpTransactionResult asserts that the fee bump transaction result is correct.
func (it *IntegrationTests) assertFeeBumpTransactionResult(ctx context.Context, req types.CreateFeeBumpTransactionRequest, resp types.TransactionEnvelopeResponse) {
	feeBumpTx, err := parseFeeBumpTxXDR(resp.Transaction)
	assertOrFail(err == nil, "parsing fee bump transaction from XDR: %v", err)

	// Assert that the inner transaction is the same as the request
	innerTxXDR, err := feeBumpTx.InnerTransaction().Base64()
	assertOrFail(err == nil, "error converting inner transaction to base64: %v", err)
	assertOrFail(innerTxXDR == req.Transaction, "inner transaction in request and response must be the same")

	// Assert that the fee bump transaction is signed by the distribution account
	assertOrFail(len(feeBumpTx.Signatures()) > 0, "fee bump transaction should be signed")
	distPubKey, err := it.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	assertOrFail(err == nil, "error getting distribution account public key: %v", err)
	distKP := keypair.MustParse(distPubKey)
	assertOrFail(feeBumpTx.Signatures()[0].Hint == distKP.Hint(), "signature at index 0 should be made by the distribution account public key")
}

// assertOrFail asserts that a condition is true. If the condition is not true, it panics with a message.
func assertOrFail(condition bool, format string, args ...any) {
	if !condition {
		panic(fmt.Sprintf("❌ Assertion failed: "+format, args...))
	}
}

func (it *IntegrationTests) createInvokeContractOp() (txnbuild.InvokeHostFunction, error) {
	var nativeAssetContractID xdr.Hash
	var err error
	nativeAssetContractID, err = xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(it.NetworkPassphrase)
	if err != nil {
		return txnbuild.InvokeHostFunction{}, fmt.Errorf("getting native asset contract ID: %w", err)
	}

	fromSCAddress, err := SCAccountID(it.SourceAccountKP.Address())
	if err != nil {
		return txnbuild.InvokeHostFunction{}, fmt.Errorf("marshalling from address: %w", err)
	}
	toSCAddress := fromSCAddress

	invokeXLMTransferSAC := txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &nativeAssetContractID,
				},
				FunctionName: "transfer",
				Args: xdr.ScVec{
					{
						Type:    xdr.ScValTypeScvAddress,
						Address: &fromSCAddress,
					},
					{
						Type:    xdr.ScValTypeScvAddress,
						Address: &toSCAddress,
					},
					{
						Type: xdr.ScValTypeScvI128,
						I128: &xdr.Int128Parts{
							Hi: xdr.Int64(0),
							Lo: xdr.Uint64(uint64(amount.MustParse("10"))),
						},
					},
				},
			},
		},
	}

	return invokeXLMTransferSAC, nil
}

func (it *IntegrationTests) prepareInvokeContractOp() (opXDR, simResultXDR string, err error) {
	invokeXLMTransferSAC, err := it.createInvokeContractOp()
	if err != nil {
		return "", "", fmt.Errorf("creating invoke contract operation: %w", err)
	}

	return it.prepareSimulateAndSignTransaction(invokeXLMTransferSAC)
}

func (it *IntegrationTests) prepareSimulateAndSignTransaction(op txnbuild.InvokeHostFunction) (opXDR, simResultXDR string, err error) {
	// Step 1: Get health to get the latest ledger
	healthResult, err := it.RPCService.GetHealth()
	if err != nil {
		return "", "", fmt.Errorf("getting health: %w", err)
	}
	latestLedger := healthResult.LatestLedger

	// Step 2: Simulation a transaction with a disposable txSourceAccount, to get the auth entries and simulation results.
	simulationSourceAccKP := keypair.MustRandom()
	simulationSourceAcc := txnbuild.SimpleAccount{AccountID: simulationSourceAccKP.Address(), Sequence: 0}
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &simulationSourceAcc,
		Operations:    []txnbuild.Operation{&op},
		BaseFee:       txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
		IncrementSequenceNum: true,
	})
	if err != nil {
		return "", "", fmt.Errorf("building transaction (1): %w", err)
	}
	txXDR, err := tx.Base64()
	if err != nil {
		return "", "", fmt.Errorf("encoding transaction to base64 (1): %w", err)
	}

	simulationResult, err := it.RPCService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
	if err != nil {
		return "", "", fmt.Errorf("simulating transaction (1): %w", err)
	}

	log.Warnf("🧪 transactionData: %+v", simulationResult.TransactionData)
	log.Warnf("🧪 auth: %+v", simulationResult.Results[0].Auth)
	log.Warnf("🧪 simulateResult(1): %+v", simulationResult)
	fmt.Println("")

	if len(simulationResult.Results) > 0 {
		// 3.1 If there are auth entries, sign them
		simulateResults := make([]entities.RPCSimulateHostFunctionResult, len(simulationResult.Results))
		for i, result := range simulationResult.Results {
			updatedResult := result
			for j, auth := range result.Auth {
				nonce, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
				if err != nil {
					return "", "", fmt.Errorf("generating random nonce: %w", err)
				}
				updatedResult.Auth[j], err = authorizeEntry(auth, nonce.Int64(), latestLedger+100, it.NetworkPassphrase, it.SourceAccountKP)
				if err != nil {
					return "", "", fmt.Errorf("signing auth at [i=%d,j=%d]: %w", i, j, err)
				}
			}

			simulateResults[i] = updatedResult
		}

		op.Auth = simulateResults[0].Auth

		// 3.2 If there are auth entries, simulate the transaction again
		tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &simulationSourceAcc,
			Operations:    []txnbuild.Operation{&op},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{
				TimeBounds: txnbuild.NewTimeout(300),
			},
			IncrementSequenceNum: true,
		})
		if err != nil {
			return "", "", fmt.Errorf("building transaction (2): %w", err)
		}
		txXDR, err = tx.Base64()
		if err != nil {
			return "", "", fmt.Errorf("encoding transaction to base64 (2): %w", err)
		}
		simulationResult, err = it.RPCService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
		if err != nil {
			return "", "", fmt.Errorf("simulating transaction (2): %w", err)
		}
	}

	opXDRObj, err := op.BuildXDR()
	if err != nil {
		return "", "", fmt.Errorf("building operation XDR: %w", err)
	}
	opXDR, err = utils.OperationXDRToBase64(opXDRObj)
	if err != nil {
		return "", "", fmt.Errorf("encoding operation XDR to base64: %w", err)
	}

	// TODO: fix this by creating a custom JSON marshaller for the simulation result.
	simResBytes, err := json.Marshal(simulationResult)
	if err != nil {
		return "", "", fmt.Errorf("encoding simulation result to JSON: %w", err)
	}

	return opXDR, string(simResBytes), nil
}

func authorizeEntry(auth xdr.SorobanAuthorizationEntry, nounce int64, validUntilLedgerSeq uint32, networkPassphrase string, authEntrySigner *keypair.Full) (xdr.SorobanAuthorizationEntry, error) {
	if auth.Credentials.Type != xdr.SorobanCredentialsTypeSorobanCredentialsAddress {
		return xdr.SorobanAuthorizationEntry{}, fmt.Errorf("unsupported credentials type %d", auth.Credentials.Type)
	}

	// 1: soroban auth entry
	entry := xdr.SorobanAuthorizationEntry{
		RootInvocation: auth.RootInvocation,
		Credentials: xdr.SorobanCredentials{
			Type: auth.Credentials.Type,
			Address: &xdr.SorobanAddressCredentials{
				Address:                   auth.Credentials.Address.Address,
				Nonce:                     xdr.Int64(nounce),
				SignatureExpirationLedger: xdr.Uint32(validUntilLedgerSeq),
				Signature:                 xdr.ScVal{}, // will be replaced
			},
		},
	}

	// 2: build preimage
	addrAuth := entry.Credentials.Address

	preimage := xdr.HashIdPreimage{
		Type: xdr.EnvelopeTypeEnvelopeTypeSorobanAuthorization,
		SorobanAuthorization: &xdr.HashIdPreimageSorobanAuthorization{
			NetworkId:                 network.ID(networkPassphrase),
			Nonce:                     addrAuth.Nonce,
			Invocation:                entry.RootInvocation,
			SignatureExpirationLedger: addrAuth.SignatureExpirationLedger,
		},
	}
	preimageBytes, err := preimage.MarshalBinary()
	if err != nil {
		return xdr.SorobanAuthorizationEntry{}, fmt.Errorf("marshalling preimage: %w", err)
	}
	payload := hash.Hash(preimageBytes)

	// 3: Produce signature
	signature, err := authEntrySigner.Sign(payload[:])
	publicKey := authEntrySigner.Address()
	if err != nil {
		return xdr.SorobanAuthorizationEntry{}, fmt.Errorf("signing payload: %w", err)
	}

	// 4: Create the signature object for the auth entry
	pubKeySymbol := xdr.ScSymbol("public_key")
	pubKeyBytes, err := strkey.Decode(strkey.VersionByteAccountID, publicKey)
	if err != nil {
		return xdr.SorobanAuthorizationEntry{}, fmt.Errorf("decoding public key: %w", err)
	}
	pubKeySCBytes := xdr.ScBytes(pubKeyBytes)
	sigSymbol := xdr.ScSymbol("signature")
	sigBytes := xdr.ScBytes(signature)
	sigMap := &xdr.ScMap{
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &pubKeySymbol,
			},
			Val: xdr.ScVal{
				Type:  xdr.ScValTypeScvBytes,
				Bytes: &pubKeySCBytes,
			},
		},
		xdr.ScMapEntry{
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &sigSymbol,
			},
			Val: xdr.ScVal{
				Type:  xdr.ScValTypeScvBytes,
				Bytes: &sigBytes,
			},
		},
	}
	sigsVector := &xdr.ScVec{
		xdr.ScVal{
			Type: xdr.ScValTypeScvMap,
			Map:  &sigMap,
		},
	}
	scSignature := xdr.ScVal{
		Type: xdr.ScValTypeScvVec,
		Vec:  &sigsVector,
	}

	// 5: Update the auth entry with the signature
	addrAuth.Signature = scSignature

	return entry, nil
}

func NewIntegrationTests(ctx context.Context, opts IntegrationTestsOptions) (*IntegrationTests, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validating integration tests options: %w", err)
	}

	go opts.RPCService.TrackRPCServiceHealth(ctx)

	return &IntegrationTests{
		BaseFee:                            opts.BaseFee,
		NetworkPassphrase:                  opts.NetworkPassphrase,
		RPCService:                         opts.RPCService,
		SourceAccountKP:                    opts.SourceAccountKP,
		WBClient:                           opts.WBClient,
		ChannelAccountStore:                store.NewChannelAccountModel(opts.DBConnectionPool),
		DBConnectionPool:                   opts.DBConnectionPool,
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		Fixtures: Fixtures{
			SourceAccountKP: opts.SourceAccountKP,
		},
	}, nil
}
