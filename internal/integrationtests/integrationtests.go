package integrationtests

import (
	"context"
	"fmt"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/pkg/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type IntegrationTestsOptions struct {
	BaseFee           int64
	NetworkPassphrase string
	RPCService        services.RPCService
	SourceAccountKP   *keypair.Full
	WBClient          *wbclient.Client
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

	return nil
}

type IntegrationTests struct {
	BaseFee           int64
	NetworkPassphrase string
	RPCService        services.RPCService
	SourceAccountKP   *keypair.Full
	WBClient          *wbclient.Client
}

func (it *IntegrationTests) Run(ctx context.Context) error {
	log.Ctx(ctx).Info("🆕 Starting integration tests...")

	// Step 1: call /tss/transactions/build
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

	// Step 2: call /tx/create-fee-bump for each transaction
	log.Ctx(ctx).Info("===> 2️⃣ Creating fee bump transaction...")
	feeBumpedTxs := make([]string, len(builtTxResponse.TransactionXDRs))
	for i, txXDR := range builtTxResponse.TransactionXDRs {
		feeBumpTxResponse, err := it.WBClient.FeeBumpTransaction(ctx, txXDR)
		if err != nil {
			return fmt.Errorf("calling feeBumpTransaction: %w", err)
		}
		log.Ctx(ctx).Infof("✅ feeBumpTxResponse[%d]: %+v", i, feeBumpTxResponse)
		feeBumpedTxs[i] = feeBumpTxResponse.Transaction
	}

	// TODO: submitTx")
	// TODO: waitForTxToBeInLedger")
	// TODO: verifyTxResult in wallet-backend")

	return nil
}

func (i *IntegrationTests) prepareBuildTxRequest() (types.BuildTransactionsRequest, error) {
	var buildTxRequest types.BuildTransactionsRequest
	paymentOp := &txnbuild.Payment{
		SourceAccount: i.SourceAccountKP.Address(),
		Destination:   i.SourceAccountKP.Address(),
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

func NewIntegrationTests(ctx context.Context, opts IntegrationTestsOptions) (*IntegrationTests, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validating integration tests options: %w", err)
	}

	go opts.RPCService.TrackRPCServiceHealth(ctx)

	return &IntegrationTests{
		BaseFee:           opts.BaseFee,
		NetworkPassphrase: opts.NetworkPassphrase,
		RPCService:        opts.RPCService,
		SourceAccountKP:   opts.SourceAccountKP,
		WBClient:          opts.WBClient,
	}, nil
}
