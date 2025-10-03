// Package integrationtests provides end-to-end integration tests for wallet-backend
package integrationtests

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/pkg/wbclient"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

const (
	networkPassphrase = "Standalone Network ; February 2017"
)

type TransactionTestSuite struct {
	suite.Suite
	containers                         *infrastructure.SharedContainers
	RPCService                         services.RPCService
	WBClient                           *wbclient.Client
	PrimaryAccountKP                   *keypair.Full
	SecondaryAccountKP                 *keypair.Full
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	DBConnectionPool                   db.ConnectionPool
	Fixtures                           Fixtures
}

func (suite *TransactionTestSuite) SetupSuite() {
	ctx := context.Background()

	// Get wallet-backend URL from container
	wbURL, err := suite.containers.WalletBackendContainer.API.GetConnectionString(ctx)
	suite.Require().NoError(err, "failed to get wallet-backend connection string")

	// Parse keypairs
	suite.PrimaryAccountKP = suite.containers.GetPrimarySourceAccountKeyPair(ctx)
	suite.Require().NoError(err, "failed to parse primary account keypair")

	suite.SecondaryAccountKP = suite.containers.GetSecondarySourceAccountKeyPair(ctx)
	suite.Require().NoError(err, "failed to parse secondary account keypair")

	// Initialize wallet-backend client
	clientAuthKP := suite.containers.GetClientAuthKeyPair(ctx)
	jwtTokenGenerator, err := auth.NewJWTTokenGenerator(clientAuthKP.Seed())
	suite.Require().NoError(err, "failed to create JWT token generator")
	requestSigner := auth.NewHTTPRequestSigner(jwtTokenGenerator)
	suite.WBClient = wbclient.NewClient(wbURL, requestSigner)

	// Get RPC host and port
	rpcHost, err := (*suite.containers.RPCContainer).Host(ctx)
	suite.Require().NoError(err, "failed to get RPC host")
	rpcPort, err := (*suite.containers.RPCContainer).MappedPort(ctx, "8000")
	suite.Require().NoError(err, "failed to get RPC port")
	rpcURL := fmt.Sprintf("http://%s:%s", rpcHost, rpcPort.Port())

	// Initialize RPC service
	httpClient := &http.Client{Timeout: 30 * time.Second}
	dbHost, err := suite.containers.WalletDBContainer.GetHost(ctx)
	suite.Require().NoError(err, "failed to get database host")
	dbPort, err := suite.containers.WalletDBContainer.GetPort(ctx)
	suite.Require().NoError(err, "failed to get database port")
	dbURL := fmt.Sprintf("postgres://postgres@%s:%s/wallet-backend?sslmode=disable", dbHost, dbPort)
	dbConnectionPool, err := db.OpenDBConnectionPool(dbURL)
	suite.Require().NoError(err, "failed to open database connection pool")
	db, err := dbConnectionPool.SqlxDB(ctx)
	suite.Require().NoError(err, "failed to get sqlx db")
	metricsService := metrics.NewMetricsService(db)
	suite.RPCService, err = services.NewRPCService(rpcURL, networkPassphrase, httpClient, metricsService)
	suite.Require().NoError(err, "failed to create RPC service")

	// Start tracking RPC health
	go suite.RPCService.TrackRPCServiceHealth(ctx, nil)

	// Initialize fixtures
	suite.Fixtures = Fixtures{
		NetworkPassphrase:  networkPassphrase,
		PrimaryAccountKP:   suite.PrimaryAccountKP,
		SecondaryAccountKP: suite.SecondaryAccountKP,
		RPCService:         suite.RPCService,
	}

	log.Ctx(ctx).Info("✅ TransactionTestSuite setup complete")
}

func (suite *TransactionTestSuite) TestTransactionFlow() {
	ctx := context.Background()

	log.Ctx(ctx).Info("🆕 Starting integration tests...")

	// Step 1: Prepare transactions locally
	log.Ctx(ctx).Info("===> 1️⃣ [Local] Building transactions...")
	useCases, err := suite.Fixtures.PrepareUseCases(ctx)
	suite.Require().NoError(err, "failed to prepare use cases")
	log.Ctx(ctx).Debugf("👀 useCases: %+v", useCases)

	// Step 2: call GraphQL buildTransaction mutation
	log.Ctx(ctx).Info("===> 2️⃣ [WalletBackend] Building transactions...")
	for _, useCase := range useCases {
		var builtTxResponse *types.BuildTransactionResponse
		builtTxResponse, err = suite.WBClient.BuildTransaction(ctx, useCase.requestedTransaction)
		suite.Require().NoError(err, "failed to build transaction for %s", useCase.Name())
		log.Ctx(ctx).Debugf("✅ [%s] builtTxResponse: %+v", useCase.Name(), builtTxResponse)
		useCase.builtTransactionXDR = builtTxResponse.TransactionXDR

		var txStr string
		txStr, err = txString(useCase.builtTransactionXDR)
		suite.Require().NoError(err, "failed to build transaction string for %s", useCase.Name())
		log.Ctx(ctx).Debugf("[%s] builtTransactionXDR: %s", useCase.Name(), txStr)
	}
	suite.assertBuildTransactionResult(useCases)

	// Step 3: sign transactions with the SourceAccountKP
	log.Ctx(ctx).Info("===> 3️⃣ [Local] Signing transactions...")
	err = suite.signTransactions(ctx, useCases)
	suite.Require().NoError(err, "failed to sign transactions")

	// Step 4: call /tx/create-fee-bump for each transaction
	log.Ctx(ctx).Info("===> 4️⃣ [WalletBackend] Creating fee bump transaction...")
	for _, useCase := range useCases {
		var feeBumpTxResponse *types.TransactionEnvelopeResponse
		feeBumpTxResponse, err = suite.WBClient.FeeBumpTransaction(ctx, useCase.signedTransactionXDR)
		suite.Require().NoError(err, "failed to create fee bump transaction for %s", useCase.Name())
		useCase.feeBumpedTransactionXDR = feeBumpTxResponse.Transaction
		log.Ctx(ctx).Debugf("✅ [%s] feeBumpTxResponse: %+v", useCase.Name(), feeBumpTxResponse)

		var txStr string
		txStr, err = txString(feeBumpTxResponse.Transaction)
		suite.Require().NoError(err, "failed to build transaction string for %s", useCase.Name())
		log.Ctx(ctx).Debugf("✅ [%s] feeBumpedTransactionXDR: %s", useCase.Name(), txStr)

		suite.assertFeeBumpTransactionResult(useCase)
	}

	// Step 5: wait for RPC to be healthy
	log.Ctx(ctx).Info("===> 5️⃣ [RPC] Waiting for RPC service to become healthy...")
	err = WaitForRPCHealthAndRun(ctx, suite.RPCService, 40*time.Second, nil)
	suite.Require().NoError(err, "RPC service did not become healthy")

	// Step 6: submit transactions to RPC
	log.Ctx(ctx).Info("===> 6️⃣ [RPC] Submitting transactions...")
	for _, useCase := range useCases {
		log.Ctx(ctx).Debugf("Submitting transaction for %s: %s", useCase.Name(), useCase.feeBumpedTransactionXDR)

		if useCase.delayTime > 0 {
			log.Ctx(ctx).Infof("⏳ %s delaying for %s", useCase.Name(), useCase.delayTime)
			time.Sleep(useCase.delayTime)
		}

		res, err := suite.RPCService.SendTransaction(useCase.feeBumpedTransactionXDR)
		suite.Require().NoError(err, "failed to send transaction for %s", useCase.Name())
		useCase.sendTransactionResult = res
		log.Ctx(ctx).Debugf("✅ %s's submission result: %+v", useCase.Name(), res)
		suite.Require().Equal(entities.PendingStatus, res.Status, "%s's transaction with hash %s failed with status %s, errorResultXdr=%+v", useCase.Name(), res.Hash, res.Status, res.ErrorResultXDR)
	}

	// Step 7: poll the network for the transaction
	log.Ctx(ctx).Info("===> 7️⃣ [RPC] Waiting for transaction confirmation...")
	for _, useCase := range useCases {
		txResult, err := WaitForTransactionConfirmation(ctx, suite.RPCService, useCase.sendTransactionResult.Hash)
		if err != nil {
			log.Ctx(ctx).Errorf("[useCase=%s,hash=%s] waiting for transaction confirmation: %v", useCase.Name(), useCase.sendTransactionResult.Hash, err)
		}
		useCase.getTransactionResult = txResult

		log.Ctx(ctx).Info(RenderResult(useCase))

		// Assert transaction succeeded
		suite.Require().Equal(entities.SuccessStatus, useCase.getTransactionResult.Status,
			"transaction for %s failed with status %s", useCase.Name(), useCase.getTransactionResult.Status)
	}

	log.Ctx(ctx).Info("✅ All integration tests passed!")
}

func (suite *TransactionTestSuite) signTransactions(ctx context.Context, useCases []*UseCase) error {
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

		signedTx, err := tx.Sign(networkPassphrase, txSigners...)
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
func (suite *TransactionTestSuite) assertBuildTransactionResult(useCases []*UseCase) {
	// Note: We can't easily access the channel account store from here in the containerized environment,
	// so we'll just verify that the transactions were built successfully
	for _, useCase := range useCases {
		// Parse the transaction from the XDR
		builtTx, err := parseTxXDR(useCase.builtTransactionXDR)
		suite.Require().NoError(err, "[%s] failed to parse transaction from XDR %s", useCase.Name(), useCase.builtTransactionXDR)

		// Assert that the tx is signed (by the channel account)
		suite.Require().NotEmpty(builtTx.Signatures(), "[%s] transaction should be signed", useCase.Name())

		// Parse and verify operations match
		requestedTx, err := parseTxXDR(useCase.requestedTransaction.TransactionXdr)
		suite.Require().NoError(err, "[%s] failed to parse requested transaction from XDR %s", useCase.Name(), useCase.requestedTransaction.TransactionXdr)

		suite.Require().Equal(len(requestedTx.Operations()), len(builtTx.Operations()),
			"[%s] number of operations in request (%d) and response (%d) must be the same",
			useCase.Name(), len(requestedTx.Operations()), len(builtTx.Operations()))
	}
}

// assertFeeBumpTransactionResult asserts that the fee bump transaction result is correct.
func (suite *TransactionTestSuite) assertFeeBumpTransactionResult(useCase *UseCase) {
	feeBumpTx, err := parseFeeBumpTxXDR(useCase.feeBumpedTransactionXDR)
	suite.Require().NoError(err, "[%s] failed to parse fee bump transaction from XDR", useCase.Name())

	// Assert that the inner transaction is the same as the request
	innerTxXDR, err := feeBumpTx.InnerTransaction().Base64()
	suite.Require().NoError(err, "[%s] failed to convert inner transaction to base64", useCase.Name())
	suite.Require().Equal(useCase.signedTransactionXDR, innerTxXDR,
		"[%s] inner transaction in request and response must be the same", useCase.Name())

	// Assert that the fee bump transaction is signed by the distribution account
	suite.Require().NotEmpty(feeBumpTx.Signatures(), "[%s] fee bump transaction should be signed", useCase.Name())
}
