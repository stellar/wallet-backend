package integrationtests

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
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
	"github.com/stellar/wallet-backend/pkg/wbclient"
)

const txTimeout = 45 * time.Second

type IntegrationTestsOptions struct {
	BaseFee                            int64
	NetworkPassphrase                  string
	RPCService                         services.RPCService
	PrimaryAccountKP                   *keypair.Full
	SecondaryAccountKP                 *keypair.Full
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
		{o.PrimaryAccountKP == nil, errors.New("primary source account keypair cannot be nil")},
		{o.SecondaryAccountKP == nil, errors.New("secondary source account keypair cannot be nil")},
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
	PrimaryAccountKP                   *keypair.Full
	SecondaryAccountKP                 *keypair.Full
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
	useCases, outerErr := it.Fixtures.PrepareUseCases(ctx)
	if outerErr != nil {
		return fmt.Errorf("preparing use cases: %w", outerErr)
	}

	log.Ctx(ctx).Debugf("👀 useCases: %+v", useCases)

	// Step 2: call GraphQL buildTransaction mutation
	fmt.Println("")
	log.Ctx(ctx).Info("===> 2️⃣ [WalletBackend] Building transactions...")
	for _, useCase := range useCases {
		builtTxResponse, err := it.WBClient.BuildTransaction(ctx, useCase.requestedTransaction)
		if err != nil {
			return fmt.Errorf("calling buildTransaction: %w", err)
		}
		log.Ctx(ctx).Debugf("✅ [%s] builtTxResponse: %+v", useCase.Name(), builtTxResponse)
		useCase.builtTransactionXDR = builtTxResponse.TransactionXDR

		txString, err := txString(useCase.builtTransactionXDR)
		if err != nil {
			return fmt.Errorf("building transaction string: %w", err)
		}
		log.Ctx(ctx).Debugf("[%s] builtTransactionXDR: %s", useCase.Name(), txString)
	}
	it.assertBuildTransactionResult(ctx, useCases)

	// Step 3: sign transactions with the SourceAccountKP
	fmt.Println("")
	log.Ctx(ctx).Info("===> 3️⃣ [Local] Signing transactions...")
	if err := it.signTransactions(ctx, useCases); err != nil {
		return fmt.Errorf("signing transactions: %w", err)
	}

	// Step 4: call /tx/create-fee-bump for each transaction
	fmt.Println("")
	log.Ctx(ctx).Info("===> 4️⃣ [WalletBackend] Creating fee bump transaction...")
	for _, useCase := range useCases {
		feeBumpTxResponse, err := it.WBClient.FeeBumpTransaction(ctx, useCase.signedTransactionXDR)
		if err != nil {
			return fmt.Errorf("calling feeBumpTransaction: %w", err)
		}
		useCase.feeBumpedTransactionXDR = feeBumpTxResponse.Transaction
		log.Ctx(ctx).Debugf("✅ [%s] feeBumpTxResponse: %+v", useCase.Name(), feeBumpTxResponse)

		txString, err := txString(feeBumpTxResponse.Transaction)
		if err != nil {
			return fmt.Errorf("building transaction string: %w", err)
		}
		log.Ctx(ctx).Debugf("✅ [%s] feeBumpedTransactionXDR: %s", useCase.Name(), txString)

		it.assertFeeBumpTransactionResult(ctx, useCase)
	}

	// Step 5: wait for RPC to be healthy
	fmt.Println("")
	log.Ctx(ctx).Info("===> 5️⃣ [RPC] Waiting for RPC service to become healthy...")
	if err := WaitForRPCHealthAndRun(ctx, it.RPCService, 40*time.Second, nil); err != nil {
		return fmt.Errorf("waiting for RPC service to become healthy: %w", err)
	}

	// Step 6: submit transactions to RPC
	fmt.Println("")
	log.Ctx(ctx).Info("===> 6️⃣ [RPC] Submitting transactions...")
	for _, useCase := range useCases {
		log.Ctx(ctx).Debugf("Submitting transaction for %s: %s", useCase.Name(), useCase.feeBumpedTransactionXDR)

		if useCase.delayTime > 0 {
			log.Ctx(ctx).Infof("⏳ %s delaying for %s", useCase.Name(), useCase.delayTime)
			time.Sleep(useCase.delayTime)
		}

		if res, err := it.RPCService.SendTransaction(useCase.feeBumpedTransactionXDR); err != nil {
			return fmt.Errorf("sending transaction for %s: %w", useCase.Name(), err)
		} else {
			useCase.sendTransactionResult = res
			log.Ctx(ctx).Debugf("✅ %s's submission result: %+v", useCase.Name(), res)
			if res.Status != entities.PendingStatus {
				return fmt.Errorf("%s's transaction with hash %s failed with status %s, errorResultXdr=%+v", useCase.Name(), res.Hash, res.Status, res.ErrorResultXDR)
			}
			useCase.sendTransactionResult = res
		}
	}

	// Step 7: poll the network for the transaction
	fmt.Println("")
	log.Ctx(ctx).Info("===> 7️⃣ [RPC] Waiting for transaction confirmation...")
	const retryDelay = 3 * time.Second
	for _, useCase := range useCases {
		txResult, err := WaitForTransactionConfirmation(ctx, it.RPCService, useCase.sendTransactionResult.Hash, retry.Delay(retryDelay), retry.Attempts(uint(txTimeout/retryDelay)))
		if err != nil {
			log.Ctx(ctx).Errorf("[useCase=%s,hash=%s] waiting for transaction confirmation: %v", useCase.Name(), useCase.sendTransactionResult.Hash, err)
		}
		useCase.getTransactionResult = txResult

		log.Ctx(ctx).Info(RenderResult(useCase))
	}

	// TODO: verifyTxResult in wallet-backend

	return nil
}

// RenderResult renders a result string for a use case.
func RenderResult(useCase *UseCase) string {
	status := useCase.getTransactionResult.Status
	var statusEmoji string
	switch status {
	case entities.SuccessStatus:
		statusEmoji = "✅"
	case entities.FailedStatus:
		statusEmoji = "❌"
	case entities.NotFoundStatus:
		statusEmoji = "⏳"
	default:
		statusEmoji = "⁉️"
	}
	statusText := fmt.Sprintf("%s %s", statusEmoji, status)

	var builder strings.Builder

	builder.WriteString(statusText)
	builder.WriteString(fmt.Sprintf(" {Use Case: %s", useCase.name))
	builder.WriteString(fmt.Sprintf(", Category: %s", useCase.category))
	builder.WriteString(fmt.Sprintf(", Hash: %s", useCase.sendTransactionResult.Hash))
	if status != entities.SuccessStatus {
		txResult := useCase.getTransactionResult
		builder.WriteString(fmt.Sprintf("ResultXDR: %+v, ErrorResultXDR: %+v, ResultMetaXDR: %+v", txResult.ResultXDR, txResult.ErrorResultXDR, txResult.ResultMetaXDR))
	}
	builder.WriteString("}")

	return builder.String()
}

func (it *IntegrationTests) signTransactions(ctx context.Context, useCases []*UseCase) error {
	for i, useCase := range useCases {
		tx, err := parseTxXDR(useCase.builtTransactionXDR)
		if err != nil {
			return fmt.Errorf("parsing transaction from XDR: %w", err)
		}

		txSigners := useCase.txSigners.Slice()
		if len(txSigners) == 0 {
			log.Ctx(ctx).Warnf("Skipping transaction signature for use case %s", useCases[i].Name())
			useCase.signedTransactionXDR = useCase.builtTransactionXDR
			continue
		}

		signedTx, err := tx.Sign(it.NetworkPassphrase, txSigners...)
		if err != nil {
			return fmt.Errorf("signing transaction: %w", err)
		}
		if useCase.signedTransactionXDR, err = signedTx.Base64(); err != nil {
			return fmt.Errorf("encoding transaction to base64: %w", err)
		}
	}

	return nil
}

// assertBuildTransactionResult asserts that the build transaction result is correct.
func (it *IntegrationTests) assertBuildTransactionResult(ctx context.Context, useCases []*UseCase) {
	for _, useCase := range useCases {
		// Parse the transaction from the XDR
		builtTx, err := parseTxXDR(useCase.builtTransactionXDR)
		assertOrFail(err == nil, "[%s] parsing transaction from XDR %s: %v", useCase.Name(), useCase.builtTransactionXDR, err)

		// Assert that the tx source account is a channel account
		txSourceAccount := builtTx.SourceAccount()
		channelAccount, err := it.ChannelAccountStore.Get(ctx, it.DBConnectionPool, txSourceAccount.GetAccountID())
		assertOrFail(err == nil, "error getting channel account: %v", err)
		assertOrFail(channelAccount != nil, "channel account not found in the database")
		// Assert that the tx is signed by the channel account
		assertOrFail(len(builtTx.Signatures()) > 0, "transaction should be signed")
		assertOrFail(builtTx.Signatures()[0].Hint == keypair.MustParse(channelAccount.PublicKey).Hint(), "signature at index 0 should be made by the channel account public key")

		// Assert the operations are the same by parsing the original transaction XDR
		requestedTx, err := parseTxXDR(useCase.requestedTransaction.TransactionXdr)
		assertOrFail(err == nil, "[%s] parsing requested transaction from XDR %s: %v", useCase.Name(), useCase.requestedTransaction.TransactionXdr, err)
		assertOpsMatch(requestedTx.Operations(), builtTx.Operations())
	}
}

func assertOpsMatch(requestOps []txnbuild.Operation, responseOps []txnbuild.Operation) {
	assertOrFail(len(requestOps) == len(responseOps), "number of operations in request (%d) and response (%d) must be the same", len(requestOps), len(responseOps))

	for j, requestOp := range requestOps {
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
func (it *IntegrationTests) assertFeeBumpTransactionResult(ctx context.Context, useCase *UseCase) {
	feeBumpTx, err := parseFeeBumpTxXDR(useCase.feeBumpedTransactionXDR)
	assertOrFail(err == nil, "parsing fee bump transaction from XDR: %v", err)

	// Assert that the inner transaction is the same as the request
	innerTxXDR, err := feeBumpTx.InnerTransaction().Base64()
	assertOrFail(err == nil, "error converting inner transaction to base64: %v", err)
	assertOrFail(innerTxXDR == useCase.signedTransactionXDR, "inner transaction in request and response must be the same")

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

	go opts.RPCService.TrackRPCServiceHealth(ctx, nil)

	fixtures := Fixtures{
		NetworkPassphrase:  opts.NetworkPassphrase,
		PrimaryAccountKP:   opts.PrimaryAccountKP,
		SecondaryAccountKP: opts.SecondaryAccountKP,
		RPCService:         opts.RPCService,
	}

	return &IntegrationTests{
		BaseFee:                            opts.BaseFee,
		NetworkPassphrase:                  opts.NetworkPassphrase,
		RPCService:                         opts.RPCService,
		PrimaryAccountKP:                   opts.PrimaryAccountKP,
		SecondaryAccountKP:                 opts.SecondaryAccountKP,
		WBClient:                           opts.WBClient,
		ChannelAccountStore:                store.NewChannelAccountModel(opts.DBConnectionPool),
		DBConnectionPool:                   opts.DBConnectionPool,
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		Fixtures:                           fixtures,
	}, nil
}
