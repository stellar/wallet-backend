package integrationtests

import (
	"context"
	"fmt"
	"reflect"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/pkg/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

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
	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	if o.NetworkPassphrase == "" {
		return fmt.Errorf("network passphrase cannot be empty")
	}

	if o.RPCService == nil {
		return fmt.Errorf("rpc client cannot be nil")
	}

	if o.SourceAccountKP == nil {
		return fmt.Errorf("source account keypair cannot be nil")
	}

	if o.WBClient == nil {
		return fmt.Errorf("wallet backend client cannot be nil")
	}

	if o.DBConnectionPool == nil {
		return fmt.Errorf("db connection pool cannot be nil")
	}

	if o.DistributionAccountSignatureClient == nil {
		return fmt.Errorf("distribution account signature client cannot be nil")
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
}

func (it *IntegrationTests) Run(ctx context.Context) error {
	log.Ctx(ctx).Info("🆕 Starting integration tests...")

	// Step 1: call /tss/transactions/build
	fmt.Println("")
	log.Ctx(ctx).Info("===> 1️⃣ Building transactions locally...")
	buildTxRequest, err := it.prepareBuildTxRequest()
	if err != nil {
		return fmt.Errorf("preparing build tx request: %w", err)
	}
	log.Ctx(ctx).Info("⏳ Calling {WalletBackend}.BuildTransactions...")
	builtTxResponse, err := it.WBClient.BuildTransactions(ctx, buildTxRequest.Transactions...)
	if err != nil {
		return fmt.Errorf("calling buildTransactions: %w", err)
	}
	log.Ctx(ctx).Infof("✅ builtTxResponse: %+v", builtTxResponse)
	for i, txXDR := range builtTxResponse.TransactionXDRs {
		txString, err := txString(txXDR)
		if err != nil {
			return fmt.Errorf("building transaction string: %w", err)
		}
		log.Ctx(ctx).Infof("builtTx[%d]: %s", i, txString)
	}
	it.assertBuildTransactionResult(ctx, buildTxRequest, *builtTxResponse)

	// Step 1.5: sign transactions with the SourceAccountKP
	signedTxXDRs := make([]string, len(builtTxResponse.TransactionXDRs))
	for i, txXDR := range builtTxResponse.TransactionXDRs {
		tx, err := parseTxXDR(txXDR)
		if err != nil {
			return fmt.Errorf("parsing transaction from XDR: %w", err)
		}
		signedTx, err := tx.Sign(it.NetworkPassphrase, it.SourceAccountKP)
		if err != nil {
			return fmt.Errorf("signing transaction: %w", err)
		}
		signedTxXDR, err := signedTx.Base64()
		if err != nil {
			return fmt.Errorf("encoding transaction to base64: %w", err)
		}
		signedTxXDRs[i] = signedTxXDR
	}

	// Step 2: call /tx/create-fee-bump for each transaction
	fmt.Println("")
	log.Ctx(ctx).Info("===> 2️⃣ Creating fee bump transaction...")
	feeBumpedTxs := make([]string, len(builtTxResponse.TransactionXDRs))
	for i, txXDR := range builtTxResponse.TransactionXDRs {
		feeBumpTxResponse, err := it.WBClient.FeeBumpTransaction(ctx, txXDR)
		if err != nil {
			return fmt.Errorf("calling feeBumpTransaction: %w", err)
		}
		log.Ctx(ctx).Infof("✅ feeBumpTxResponse[%d]: %+v", i, feeBumpTxResponse)
		feeBumpedTxs[i] = feeBumpTxResponse.Transaction

		txString, err := txString(feeBumpedTxs[i])
		if err != nil {
			return fmt.Errorf("building transaction string: %w", err)
		}
		log.Ctx(ctx).Infof("feeBumpedTx[%d]: %s", i, txString)

		it.assertFeeBumpTransactionResult(ctx, types.CreateFeeBumpTransactionRequest{Transaction: txXDR}, *feeBumpTxResponse)
	}

	// TODO: submitTx")
	// TODO: waitForTxToBeInLedger")
	// TODO: verifyTxResult in wallet-backend")

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

// prepareBuildTxRequest prepares a build transaction request with a payment operation.
func (it *IntegrationTests) prepareBuildTxRequest() (types.BuildTransactionsRequest, error) {
	var buildTxRequest types.BuildTransactionsRequest
	paymentOp := &txnbuild.Payment{
		SourceAccount: it.SourceAccountKP.Address(),
		Destination:   it.SourceAccountKP.Address(),
		Amount:        "10000000",
		Asset:         txnbuild.NativeAsset{},
	}

	paymentOpXDR, err := paymentOp.BuildXDR()
	if err != nil {
		return buildTxRequest, fmt.Errorf("building payment operation XDR: %w", err)
	}
	b64OpXDR, err := utils.OperationXDRToBase64(paymentOpXDR)
	if err != nil {
		return buildTxRequest, fmt.Errorf("encoding payment operation XDR to base64: %w", err)
	}

	buildTxRequest.Transactions = append(buildTxRequest.Transactions, types.Transaction{
		Operations: []string{b64OpXDR},
	})

	return buildTxRequest, nil
}

// txString returns a string representation of a transaction given its XDR.
func txString(txXDR string) (string, error) {
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return "", fmt.Errorf("building transaction from XDR: %w", err)
	}

	opsSliceStr := func(ops []txnbuild.Operation) []string {
		var opsStr []string
		for _, op := range ops {
			opsStr = append(opsStr, fmt.Sprintf("\n\t\t%#v", op))
		}
		return opsStr
	}

	if tx, ok := genericTx.Transaction(); ok {
		return fmt.Sprintf("\n\ttx=%#v, \n\tops=%+v", tx, opsSliceStr(tx.Operations())), nil
	} else if feeBumpTx, ok := genericTx.FeeBump(); ok {
		return fmt.Sprintf("\n\tfeeBump=%#v, \n\ttx=%#v, \n\tops=%+v", feeBumpTx, feeBumpTx.InnerTransaction(), opsSliceStr(feeBumpTx.InnerTransaction().Operations())), nil
	}

	return fmt.Sprintf("%+v", genericTx), nil
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
	}, nil
}
