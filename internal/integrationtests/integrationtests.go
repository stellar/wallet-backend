package integrationtests

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/tss"
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
	useCases, err := it.Fixtures.PrepareUseCases(ctx)
	if err != nil {
		return fmt.Errorf("preparing use cases: %w", err)
	}

	buildTxRequest := types.BuildTransactionsRequest{Transactions: []types.Transaction{}}
	log.Ctx(ctx).Debugf("👀 useCases: %+v", useCases)
	for i, useCase := range useCases {
		log.Ctx(ctx).Infof("👀 useCase[%d]: %+v", i, useCase.name)
		buildTxRequest.Transactions = append(buildTxRequest.Transactions, useCase.requestedTransaction)
	}

	// Step 2: call /tss/transactions/build
	fmt.Println("")
	log.Ctx(ctx).Info("===> 2️⃣ [WalletBackend] Building transactions...")
	builtTxResponse, err := it.WBClient.BuildTransactions(ctx, buildTxRequest.Transactions...)
	if err != nil {
		return fmt.Errorf("calling buildTransactions: %w", err)
	}
	log.Ctx(ctx).Debugf("✅ builtTxResponse: %+v", builtTxResponse)
	for i, txXDR := range builtTxResponse.TransactionXDRs {
		useCases[i].builtTransactionXDR = txXDR

		txString, innerErr := txString(txXDR)
		if innerErr != nil {
			return fmt.Errorf("building transaction string: %w", innerErr)
		}
		log.Ctx(ctx).Debugf("builtTx[%d]: %s", i, txString)
	}
	it.assertBuildTransactionResult(ctx, buildTxRequest, *builtTxResponse)

	// Step 3: sign transactions with the SourceAccountKP
	fmt.Println("")
	log.Ctx(ctx).Info("===> 3️⃣ [Local] Signing transactions...")
	signedTxXDRs, err := it.signTransactions(ctx, builtTxResponse)
	if err != nil {
		return fmt.Errorf("signing transactions: %w", err)
	}
	for i := range useCases {
		useCases[i].signedTransactionXDR = signedTxXDRs[i]
	}

	// Step 4: call /tx/create-fee-bump for each transaction
	fmt.Println("")
	log.Ctx(ctx).Info("===> 4️⃣ [WalletBackend] Creating fee bump transaction...")
	for i, txXDR := range signedTxXDRs {
		feeBumpTxResponse, innerErr := it.WBClient.FeeBumpTransaction(ctx, txXDR)
		if innerErr != nil {
			return fmt.Errorf("calling feeBumpTransaction: %w", innerErr)
		}
		log.Ctx(ctx).Debugf("✅ feeBumpTxResponse[%d]: %+v", i, feeBumpTxResponse)
		useCases[i].feeBumpedTransactionXDR = feeBumpTxResponse.Transaction

		txString, innerErr := txString(feeBumpTxResponse.Transaction)
		if innerErr != nil {
			return fmt.Errorf("building transaction string: %w", innerErr)
		}
		log.Ctx(ctx).Debugf("feeBumpedTx[%d]: %s", i, txString)

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
	for i, useCase := range useCases {
		log.Ctx(ctx).Debugf("Submitting transaction %d: %s", i, useCase.feeBumpedTransactionXDR)

		if res, err := it.RPCService.SendTransaction(useCase.feeBumpedTransactionXDR); err != nil {
			return fmt.Errorf("sending transaction %d: %w", i, err)
		} else {
			log.Ctx(ctx).Debugf("✅ submittedTx[%d]: %+v", i, res)
			if res.Status != entities.PendingStatus {
				errResult, err := tss.UnmarshallTransactionResultXDR(res.ErrorResultXDR)
				if err != nil {
					return fmt.Errorf("unmarshalling transaction result XDR: %w", err)
				}

				return fmt.Errorf("transaction %d with hash %s failed with status %s, errorResultXdr=%s, errorResult=%+v, innerResultPair=%+v", i, res.Hash, res.Status, res.ErrorResultXDR, errResult, errResult.Result.InnerResultPair)
			}
			useCases[i].feeBumpedTransactionHash = res.Hash
		}
	}

	// Step 7: poll the network for the transaction
	fmt.Println("")
	log.Ctx(ctx).Info("===> 7️⃣ [RPC] Waiting for transaction confirmation...")
	const retryDelay = 6 * time.Second
	for i, useCase := range useCases {
		prefix := fmt.Sprintf("[%d - name=%s,hash=%s]", i, useCase.name, useCase.feeBumpedTransactionHash)
		txResult, err := WaitForTransactionConfirmation(ctx, it.RPCService, useCase.feeBumpedTransactionHash, retry.Delay(retryDelay), retry.Attempts(uint(txTimeout/retryDelay)))
		if err != nil {
			log.Ctx(ctx).Errorf("%s waiting for transaction confirmation: %v", prefix, err)
		}
		if txResult.Status == entities.SuccessStatus {
			log.Ctx(ctx).Infof("✅ %s confirmed on Stellar network", prefix)
		} else {
			errResult, err := tss.UnmarshallTransactionResultXDR(txResult.ErrorResultXDR)
			if err != nil {
				return fmt.Errorf("unmarshalling transaction result XDR: %w", err)
			}
			log.Ctx(ctx).Errorf("🔴 %s failed with status=%s and errorResultXdr=%+v", prefix, txResult.Status, errResult)
		}
	}

	// TODO: verifyTxResult in wallet-backend

	return nil
}

func (it *IntegrationTests) signTransactions(ctx context.Context, builtTxResponse *types.BuildTransactionsResponse) ([]string, error) {
	signedTxXDRs := make([]string, len(builtTxResponse.TransactionXDRs))

	for i, txXDR := range builtTxResponse.TransactionXDRs {
		tx, err := parseTxXDR(txXDR)
		if err != nil {
			return nil, fmt.Errorf("parsing transaction from XDR: %w", err)
		}

		if utils.IsSorobanTxnbuildOp(tx.Operations()[0]) && tx.Operations()[0].GetSourceAccount() != it.SourceAccountKP.Address() {
			log.Ctx(ctx).Warnf("Skipping signature for Soroban transaction at index %d", i)
			signedTxXDRs[i] = txXDR
			continue
		}

		signedTx, err := tx.Sign(it.NetworkPassphrase, it.SourceAccountKP)
		if err != nil {
			return nil, fmt.Errorf("signing transaction: %w", err)
		}
		if signedTxXDRs[i], err = signedTx.Base64(); err != nil {
			return nil, fmt.Errorf("encoding transaction to base64: %w", err)
		}
	}

	return signedTxXDRs, nil
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
		assertOpsMatch(req.Transactions[i].Operations, tx.Operations())
	}
}

func assertOpsMatch(requestOpsXDRs []string, responseOps []txnbuild.Operation) {
	for j, requestOpXDRStr := range requestOpsXDRs {
		requestOpXDR, err := utils.OperationXDRFromBase64(requestOpXDRStr)
		assertOrFail(err == nil, "error converting operation string to XDR: %v", err)
		requestOp, err := utils.OperationXDRToTxnBuildOp(requestOpXDR)
		assertOrFail(err == nil, "error converting operation XDR to txnbuild operation: %v", err)

		responseOp := responseOps[j]
		// In case of invokeContractOp, we set the Ext of the request to the same as in the response.
		// This is because the response includes the transaction data, which is not present in the request.
		// It cannot be added to the request yet, until we add a new field for that.
		if invokeContractOpRequest, ok := requestOp.(*txnbuild.InvokeHostFunction); ok {
			invokeContractOpResponse, ok := responseOp.(*txnbuild.InvokeHostFunction)
			assertOrFail(ok, "operation %d in request is an invokeContractOp but response is not", j)
			invokeContractOpRequest.Ext = invokeContractOpResponse.Ext
			requestOp = invokeContractOpRequest
			responseOp = invokeContractOpResponse
		}
		assertOrFail(reflect.DeepEqual(requestOp, responseOp), "operation %d in request and response must be the same", j)
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

func NewIntegrationTests(ctx context.Context, opts IntegrationTestsOptions) (*IntegrationTests, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validating integration tests options: %w", err)
	}

	go opts.RPCService.TrackRPCServiceHealth(ctx)

	fixtures := Fixtures{
		NetworkPassphrase: opts.NetworkPassphrase,
		SourceAccountKP:   opts.SourceAccountKP,
		RPCService:        opts.RPCService,
	}

	return &IntegrationTests{
		BaseFee:                            opts.BaseFee,
		NetworkPassphrase:                  opts.NetworkPassphrase,
		RPCService:                         opts.RPCService,
		SourceAccountKP:                    opts.SourceAccountKP,
		WBClient:                           opts.WBClient,
		ChannelAccountStore:                store.NewChannelAccountModel(opts.DBConnectionPool),
		DBConnectionPool:                   opts.DBConnectionPool,
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		Fixtures:                           fixtures,
	}, nil
}
