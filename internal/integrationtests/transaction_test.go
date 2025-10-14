// Package integrationtests provides end-to-end integration tests for wallet-backend
package integrationtests

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go/support/log"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

const (
	networkPassphrase = "Standalone Network ; February 2017"
)

type BuildAndSubmitTransactionsTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
	pool    pond.Pool
}

func (suite *BuildAndSubmitTransactionsTestSuite) SetupSuite() {
	suite.pool = pond.NewPool(0)
}

func (suite *BuildAndSubmitTransactionsTestSuite) TearDownSuite() {
	if suite.pool != nil {
		suite.pool.StopAndWait()
	}
}

func (suite *BuildAndSubmitTransactionsTestSuite) TestBuildSignAndSubmitTransactions() {
	ctx := context.Background()

	// Build transactions in parallel
	log.Ctx(ctx).Info("===> 1️⃣ [WalletBackend] Building transactions...")

	group := suite.pool.NewGroupContext(ctx)
	var mu sync.Mutex
	var errs []error

	for _, useCase := range suite.testEnv.UseCases {
		uc := useCase
		group.Submit(func() {
			builtTxResponse, err := suite.testEnv.WBClient.BuildTransaction(ctx, uc.RequestedTransaction)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to build transaction for %s: %w", uc.Name(), err))
				mu.Unlock()
				return
			}

			mu.Lock()
			uc.BuiltTransactionXDR = builtTxResponse.TransactionXDR
			mu.Unlock()

			log.Ctx(ctx).Debugf("✅ [%s] builtTxResponse: %+v", uc.Name(), builtTxResponse)

			_, err = txString(uc.BuiltTransactionXDR)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to build transaction string for %s: %w", uc.Name(), err))
				mu.Unlock()
				return
			}
		})
	}

	err := group.Wait()
	suite.Require().NoError(err)
	suite.Require().Empty(errs)

	suite.assertBuildTransactionResult()

	// Sign transactions in parallel
	log.Ctx(ctx).Info("===> 2️⃣ [Local] Signing transactions...")
	suite.signTransactions(ctx, suite.testEnv.UseCases)

	// Create fee bump transactions in parallel
	log.Ctx(ctx).Info("===> 3️⃣ [WalletBackend] Creating fee bump transactions...")
	suite.createFeeBumpTransactions(ctx, suite.testEnv.UseCases)

	// Submit transactions in parallel
	log.Ctx(ctx).Info("===> 4️⃣ [RPC] Submitting transactions...")
	suite.submitTransactions(ctx)
}

func (suite *BuildAndSubmitTransactionsTestSuite) createFeeBumpTransactions(ctx context.Context, useCases []*infrastructure.UseCase) {
	group := suite.pool.NewGroupContext(ctx)
	var mu sync.Mutex
	var errs []error

	for _, useCase := range useCases {
		uc := useCase
		group.Submit(func() {
			feeBumpTxResponse, err := suite.testEnv.WBClient.FeeBumpTransaction(ctx, uc.SignedTransactionXDR)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to create fee bump transaction for %s: %w", uc.Name(), err))
				mu.Unlock()
				return
			}

			mu.Lock()
			uc.FeeBumpedTransactionXDR = feeBumpTxResponse.Transaction
			mu.Unlock()

			log.Ctx(ctx).Debugf("✅ [%s] feeBumpTxResponse: %+v", uc.Name(), feeBumpTxResponse)

			txStr, err := txString(feeBumpTxResponse.Transaction)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to build transaction string for %s: %w", uc.Name(), err))
				mu.Unlock()
				return
			}
			log.Ctx(ctx).Debugf("✅ [%s] feeBumpedTransactionXDR: %s", uc.Name(), txStr)

			suite.assertFeeBumpTransactionResult(uc)
		})
	}

	err := group.Wait()
	suite.Require().NoError(err)
	suite.Require().Empty(errs)
}

func (suite *BuildAndSubmitTransactionsTestSuite) submitTransactions(ctx context.Context) {
	submitTransaction := func(useCase *infrastructure.UseCase) {
		log.Ctx(ctx).Debugf("Submitting transaction for %s: %s", useCase.Name(), useCase.FeeBumpedTransactionXDR)
		if useCase.DelayTime > 0 {
			log.Ctx(ctx).Infof("⏳ %s delaying for %s", useCase.Name(), useCase.DelayTime)
			time.Sleep(useCase.DelayTime)
		}
		res, sendErr := suite.testEnv.RPCService.SendTransaction(useCase.FeeBumpedTransactionXDR)
		suite.Require().NoError(sendErr, "failed to send transaction for %s", useCase.Name())
		useCase.SendTransactionResult = res
		log.Ctx(ctx).Debugf("✅ %s's submission result: %+v", useCase.Name(), res)
		suite.Require().Equal(entities.PendingStatus, res.Status, "%s's transaction with hash %s failed with status %s, errorResultXdr=%+v", useCase.Name(), res.Hash, res.Status, res.ErrorResultXDR)
	}

	// PHASE A: Wait for RPC health
	log.Ctx(ctx).Info("===> 4️⃣ [RPC] Waiting for RPC service to become healthy...")
	if err := infrastructure.WaitForRPCHealthAndRun(ctx, suite.testEnv.RPCService, 40*time.Second, nil); err != nil {
		suite.Require().NoError(err, "RPC service did not become healthy")
	}

	// PHASE B: Submit all regular transactions (INCLUDING createClaimableBalanceOps)
	log.Ctx(ctx).Info("===> 5️⃣ [RPC] Submitting transactions...")
	for _, useCase := range suite.testEnv.UseCases {
		submitTransaction(useCase)
	}

	// PHASE C: Wait for all transactions to confirm
	log.Ctx(ctx).Info("===> 6️⃣ [RPC] Waiting for transaction confirmation...")
	group := suite.pool.NewGroupContext(ctx)
	var mu sync.Mutex
	var errs []error
	var failedUseCases []*entities.RPCGetTransactionResult

	for _, useCase := range suite.testEnv.UseCases {
		uc := useCase
		group.Submit(func() {
			txResult, confirmErr := infrastructure.WaitForTransactionConfirmation(ctx, suite.testEnv.RPCService, uc.SendTransactionResult.Hash)
			if confirmErr != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("[useCase=%s,hash=%s] waiting for transaction confirmation: %w", uc.Name(), uc.SendTransactionResult.Hash, confirmErr))
				mu.Unlock()
				return
			}

			mu.Lock()
			uc.GetTransactionResult = txResult
			mu.Unlock()

			log.Ctx(ctx).Info(infrastructure.RenderResult(uc))

			// Assert transaction succeeded
			if txResult.Status == entities.FailedStatus {
				mu.Lock()
				failedUseCases = append(failedUseCases, &txResult)
				mu.Unlock()
			}
		})
	}

	err := group.Wait()
	suite.Require().NoError(err)
	suite.Require().Empty(errs)
	suite.Require().Empty(failedUseCases)

	// PHASE D: Extract balance IDs from createClaimableBalanceOps result
	log.Ctx(ctx).Info("===> 7️⃣ [Processing] Extracting claimable balance IDs from confirmed transaction...")
	createCBUseCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/createClaimableBalanceOps")
	suite.Require().NotNil(createCBUseCase, "createClaimableBalanceOps use case not found")

	balanceIDs, err := infrastructure.ExtractClaimableBalanceIDsFromMeta(createCBUseCase.GetTransactionResult.ResultMetaXDR)
	suite.Require().NoError(err, "failed to extract claimable balance IDs")
	suite.Require().Len(balanceIDs, 2, "expected 2 claimable balance IDs")

	log.Ctx(ctx).Infof("✅ Extracted balance IDs: [0]=%s, [1]=%s", balanceIDs[0], balanceIDs[1])

	// PHASE E: Create claim/clawback use cases with real balance IDs
	log.Ctx(ctx).Info("===> 8️⃣ [WalletBackend] Creating claim, clawback, and clear auth flags use cases with real balance IDs...")
	fixtures := &infrastructure.Fixtures{
		NetworkPassphrase:     suite.testEnv.NetworkPassphrase,
		PrimaryAccountKP:      suite.testEnv.PrimaryAccountKP,
		SecondaryAccountKP:    suite.testEnv.SecondaryAccountKP,
		SponsoredNewAccountKP: suite.testEnv.SponsoredNewAccountKP,
		RPCService:            suite.testEnv.RPCService,
	}
	claimAndClawbackUseCases, err := fixtures.PrepareClaimAndClawbackUseCases(
		balanceIDs[0], // first balance to claim
		balanceIDs[1], // second balance to clawback
	)
	suite.Require().NoError(err, "failed to prepare claim, clawback, and clear auth flags use cases")
	suite.Require().Len(claimAndClawbackUseCases, 3)

	// PHASE F: Build transactions for claim/clawback
	log.Ctx(ctx).Info("===> 9️⃣ [WalletBackend] Building claim, clawback, and clear auth flags transactions...")
	group = suite.pool.NewGroupContext(ctx)
	errs = []error{}
	for _, useCase := range claimAndClawbackUseCases {
		uc := useCase
		group.Submit(func() {
			builtTxResponse, err := suite.testEnv.WBClient.BuildTransaction(ctx, uc.RequestedTransaction)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to build transaction for %s: %w", uc.Name(), err))
				mu.Unlock()
				return
			}
			mu.Lock()
			uc.BuiltTransactionXDR = builtTxResponse.TransactionXDR
			mu.Unlock()
		})
	}
	err = group.Wait()
	suite.Require().NoError(err)
	suite.Require().Empty(errs)

	// PHASE G: Sign transactions (reuse existing function)
	log.Ctx(ctx).Info("===> 🔟 [Local] Signing claim, clawback, and clear auth flags transactions...")
	suite.signTransactions(ctx, claimAndClawbackUseCases)

	// PHASE H: Create fee bump transactions (reuse existing function)
	log.Ctx(ctx).Info("===> 1️⃣1️⃣ [WalletBackend] Creating fee bump for claim, clawback, and clear auth flags transactions...")
	suite.createFeeBumpTransactions(ctx, claimAndClawbackUseCases)

	// PHASE I: Submit claim/clawback transactions
	log.Ctx(ctx).Info("===> 1️⃣2️⃣ [RPC] Submitting claim, clawback, and clear auth flags transactions...")
	for _, uc := range claimAndClawbackUseCases {
		submitTransaction(uc)
	}

	// PHASE J: Wait for claim/clawback confirmations
	log.Ctx(ctx).Info("===> 1️⃣3️⃣ [RPC] Waiting for claim, clawback, and clear auth flags transaction confirmation...")
	for _, uc := range claimAndClawbackUseCases {
		txResult, confirmErr := infrastructure.WaitForTransactionConfirmation(ctx, suite.testEnv.RPCService, uc.SendTransactionResult.Hash)
		suite.Require().NoError(confirmErr, "failed to wait for transaction confirmation for %s", uc.Name())
		uc.GetTransactionResult = txResult
		log.Ctx(ctx).Info(infrastructure.RenderResult(uc))
		suite.Require().Equal(entities.SuccessStatus, txResult.Status, "transaction for %s failed", uc.Name())
	}
}

func (suite *BuildAndSubmitTransactionsTestSuite) signTransactions(ctx context.Context, useCases []*infrastructure.UseCase) {
	group := suite.pool.NewGroupContext(ctx)
	var mu sync.Mutex
	var errs []error

	for _, useCase := range useCases {
		uc := useCase
		group.Submit(func() {
			tx, err := parseTxXDR(uc.BuiltTransactionXDR)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("parsing transaction from XDR: %w", err))
				mu.Unlock()
				return
			}

			txSigners := uc.TxSigners.Slice()
			if len(txSigners) == 0 {
				log.Ctx(ctx).Warnf("Skipping transaction signature for use case %s", uc.Name())
				mu.Lock()
				uc.SignedTransactionXDR = uc.BuiltTransactionXDR
				mu.Unlock()
				return
			}

			signedTx, err := tx.Sign(networkPassphrase, txSigners...)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("signing transaction: %w", err))
				mu.Unlock()
				return
			}

			signedXDR, err := signedTx.Base64()
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("encoding transaction to base64: %w", err))
				mu.Unlock()
				return
			}

			mu.Lock()
			uc.SignedTransactionXDR = signedXDR
			mu.Unlock()
		})
	}

	err := group.Wait()
	suite.Require().NoError(err)
	suite.Require().Empty(errs)
}

// assertBuildTransactionResult asserts that the build transaction result is correct.
func (suite *BuildAndSubmitTransactionsTestSuite) assertBuildTransactionResult() {
	// Note: We can't easily access the channel account store from here in the containerized environment,
	// so we'll just verify that the transactions were built successfully
	for _, useCase := range suite.testEnv.UseCases {
		// Parse the transaction from the XDR
		builtTx, err := parseTxXDR(useCase.BuiltTransactionXDR)
		suite.Require().NoError(err, "[%s] failed to parse transaction from XDR %s", useCase.Name(), useCase.BuiltTransactionXDR)

		// Assert that the tx is signed (by the channel account)
		suite.Require().NotEmpty(builtTx.Signatures(), "[%s] transaction should be signed", useCase.Name())

		// Parse and verify operations match
		requestedTx, err := parseTxXDR(useCase.RequestedTransaction.TransactionXdr)
		suite.Require().NoError(err, "[%s] failed to parse requested transaction from XDR %s", useCase.Name(), useCase.RequestedTransaction.TransactionXdr)

		suite.Require().Equal(len(requestedTx.Operations()), len(builtTx.Operations()),
			"[%s] number of operations in request (%d) and response (%d) must be the same",
			useCase.Name(), len(requestedTx.Operations()), len(builtTx.Operations()))
	}
}

// assertFeeBumpTransactionResult asserts that the fee bump transaction result is correct.
func (suite *BuildAndSubmitTransactionsTestSuite) assertFeeBumpTransactionResult(useCase *infrastructure.UseCase) {
	feeBumpTx, err := parseFeeBumpTxXDR(useCase.FeeBumpedTransactionXDR)
	suite.Require().NoError(err, "[%s] failed to parse fee bump transaction from XDR", useCase.Name())

	// Assert that the inner transaction is the same as the request
	innerTxXDR, err := feeBumpTx.InnerTransaction().Base64()
	suite.Require().NoError(err, "[%s] failed to convert inner transaction to base64", useCase.Name())
	suite.Require().Equal(useCase.SignedTransactionXDR, innerTxXDR,
		"[%s] inner transaction in request and response must be the same", useCase.Name())

	// Assert that the fee bump transaction is signed by the distribution account
	suite.Require().NotEmpty(feeBumpTx.Signatures(), "[%s] fee bump transaction should be signed", useCase.Name())
}
