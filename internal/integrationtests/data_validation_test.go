// Package integrationtests provides end-to-end integration tests for wallet-backend
package integrationtests

import (
	"context"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

var xlmAsset = xdr.MustNewNativeAsset()

type DataValidationTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

// findUseCase finds a use case by name from the test environment
func findUseCase(suite *DataValidationTestSuite, useCaseName string) *infrastructure.UseCase {
	for _, uc := range suite.testEnv.UseCases {
		if uc.Name() == useCaseName {
			return uc
		}
	}
	return nil
}

// validateTransactionBase validates common transaction fields
func validateTransactionBase(suite *DataValidationTestSuite, ctx context.Context, txHash string) *types.GraphQLTransaction {
	tx, err := suite.testEnv.WBClient.GetTransactionByHash(ctx, txHash)
	suite.Require().NoError(err, "failed to get transaction by hash")
	suite.Require().NotNil(tx, "transaction should not be nil")

	// Verify transaction fields
	suite.Require().Equal(txHash, tx.Hash, "transaction hash mismatch")
	suite.Require().NotEmpty(tx.EnvelopeXdr, "envelope XDR should not be empty")
	suite.Require().NotEmpty(tx.ResultXdr, "result XDR should not be empty")
	suite.Require().NotEmpty(tx.MetaXdr, "meta XDR should not be empty")
	suite.Require().NotZero(tx.LedgerNumber, "ledger number should not be zero")
	suite.Require().False(tx.LedgerCreatedAt.IsZero(), "ledger created at should not be zero")
	suite.Require().False(tx.IngestedAt.IsZero(), "ingested at should not be zero")

	return tx
}

// validateOperationBase validates common operation fields
func validateOperationBase(suite *DataValidationTestSuite, op *types.Operation) {
	suite.Require().NotNil(op, "operation should not be nil")
	suite.Require().NotEmpty(op.OperationXdr, "operation XDR should not be empty")
	suite.Require().NotZero(op.LedgerNumber, "ledger number should not be zero")
	suite.Require().False(op.LedgerCreatedAt.IsZero(), "ledger created at should not be zero")
	suite.Require().False(op.IngestedAt.IsZero(), "ingested at should not be zero")
}

// validateStateChangeBase validates common state change fields
func validateStateChangeBase(suite *DataValidationTestSuite, sc types.StateChangeNode, expectedLedger int64) {
	suite.Require().NotNil(sc, "state change should not be nil")
	suite.Require().Equal(expectedLedger, int64(sc.GetLedgerNumber()), "ledger number mismatch")
	suite.Require().False(sc.GetLedgerCreatedAt().IsZero(), "ledger created at should not be zero")
	suite.Require().False(sc.GetIngestedAt().IsZero(), "ingested at should not be zero")
}

// validateBalanceChange validates a balance state change
func validateBalanceChange(suite *DataValidationTestSuite, bc *types.StandardBalanceChange, expectedTokenID, expectedAmount, expectedAccount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(bc, "balance change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryBalance, bc.GetType(), "should be BALANCE type")
	suite.Require().Equal(expectedReason, bc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedTokenID, bc.TokenID, "token ID mismatch")
	suite.Require().Equal(expectedAmount, bc.Amount, "amount mismatch")
	suite.Require().Equal(expectedAccount, bc.GetAccountID(), "account ID mismatch")
}

// validateAccountChange validates an account state change
func validateAccountChange(suite *DataValidationTestSuite, ac *types.AccountChange, expectedTokenID, expectedAmount, expectedAccount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(ac, "account change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryAccount, ac.GetType(), "should be ACCOUNT type")
	suite.Require().Equal(expectedReason, ac.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedTokenID, ac.TokenID, "token ID mismatch")
	suite.Require().Equal(expectedAmount, ac.Amount, "amount mismatch")
	suite.Require().Equal(expectedAccount, ac.GetAccountID(), "account ID mismatch")
}

// validateSignerChange validates a signer state change
func validateSignerChange(suite *DataValidationTestSuite, sc *types.SignerChange, expectedAccount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(sc, "signer change should not be nil")
	suite.Require().Equal(types.StateChangeCategorySigner, sc.GetType(), "should be SIGNER type")
	suite.Require().Equal(expectedReason, sc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedAccount, sc.GetAccountID(), "account ID mismatch")
	// SignerAddress and SignerWeights can be nil for REMOVE operations
	if expectedReason != types.StateChangeReasonRemove {
		suite.Require().NotNil(sc.SignerAddress, "signer address should not be nil")
		suite.Require().NotNil(sc.SignerWeights, "signer weights should not be nil")
		suite.Require().NotEmpty(*sc.SignerAddress, "signer address should not be empty")
		suite.Require().NotEmpty(*sc.SignerWeights, "signer weights should not be empty")
	}
}

// validateSignerThresholdsChange validates a signer thresholds state change
func validateSignerThresholdsChange(suite *DataValidationTestSuite, stc *types.SignerThresholdsChange, expectedAccount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(stc, "signer thresholds change should not be nil")
	suite.Require().Equal(types.StateChangeCategorySignatureThreshold, stc.GetType(), "should be SIGNATURE_THRESHOLD type")
	suite.Require().Equal(expectedReason, stc.GetReason(), "reason mismatch")
	suite.Require().NotEmpty(stc.Thresholds, "thresholds should not be empty")
	suite.Require().Equal(expectedAccount, stc.GetAccountID(), "account ID mismatch")
}

// validateMetadataChange validates a metadata state change
func validateMetadataChange(suite *DataValidationTestSuite, mc *types.MetadataChange, expectedAccount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(mc, "metadata change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryMetadata, mc.GetType(), "should be METADATA type")
	suite.Require().Equal(expectedReason, mc.GetReason(), "reason mismatch")
	suite.Require().NotEmpty(mc.KeyValue, "key value should not be empty")
	suite.Require().Equal(expectedAccount, mc.GetAccountID(), "account ID mismatch")
}

// validateFlagsChange validates a flags state change
func validateFlagsChange(suite *DataValidationTestSuite, fc *types.FlagsChange, expectedAccount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(fc, "flags change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryFlags, fc.GetType(), "should be FLAGS type")
	suite.Require().Equal(expectedReason, fc.GetReason(), "reason mismatch")
	suite.Require().NotNil(fc.Flags, "flags should not be nil")
	suite.Require().NotEmpty(fc.Flags, "flags should not be empty")
	suite.Require().Equal(expectedAccount, fc.GetAccountID(), "account ID mismatch")
}

// validateTrustlineChange validates a trustline state change
func validateTrustlineChange(suite *DataValidationTestSuite, tc *types.TrustlineChange, expectedAccount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(tc, "trustline change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryTrustline, tc.GetType(), "should be TRUSTLINE type")
	suite.Require().Equal(expectedReason, tc.GetReason(), "reason mismatch")
	suite.Require().NotEmpty(tc.Limit, "limit should not be empty")
	suite.Require().Equal(expectedAccount, tc.GetAccountID(), "account ID mismatch")
}

// validateReservesChange validates a reserves state change
func validateReservesChange(suite *DataValidationTestSuite, rc *types.ReservesChange, expectedAccount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(rc, "reserves change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryReserves, rc.GetType(), "should be RESERVES type")
	suite.Require().Equal(expectedReason, rc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedAccount, rc.GetAccountID(), "account ID mismatch")
	// At least one of SponsoredAddress or SponsorAddress should be populated
	hasSponsored := rc.SponsoredAddress != nil && *rc.SponsoredAddress != ""
	hasSponsor := rc.SponsorAddress != nil && *rc.SponsorAddress != ""
	suite.Require().True(hasSponsored || hasSponsor, "either sponsored address or sponsor address should be populated")
}

// validateBalanceAuthorizationChange validates a balance authorization state change
func validateBalanceAuthorizationChange(suite *DataValidationTestSuite, bac *types.BalanceAuthorizationChange, expectedAccount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(bac, "balance authorization change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryBalanceAuthorization, bac.GetType(), "should be BALANCE_AUTHORIZATION type")
	suite.Require().Equal(expectedReason, bac.GetReason(), "reason mismatch")
	suite.Require().NotNil(bac.Flags, "flags should not be nil")
	suite.Require().NotEmpty(bac.Flags, "flags should not be empty")
	suite.Require().Equal(expectedAccount, bac.GetAccountID(), "account ID mismatch")
}

// assertStateChangeCounts asserts the count of state changes by reason
func assertStateChangeCounts(suite *DataValidationTestSuite, stateChanges *types.StateChangeConnection, expectedCounts map[types.StateChangeReason]int) {
	actualCounts := make(map[types.StateChangeReason]int)

	for _, edge := range stateChanges.Edges {
		suite.Require().NotNil(edge.Node, "state change node should not be nil")
		reason := edge.Node.GetReason()
		actualCounts[reason]++
	}

	for reason, expectedCount := range expectedCounts {
		actualCount := actualCounts[reason]
		suite.Require().Equal(expectedCount, actualCount, "mismatch for reason %s", reason)
	}
}

// assertStateChangeCategories asserts the count of state changes by category
func assertStateChangeCategories(suite *DataValidationTestSuite, stateChanges *types.StateChangeConnection, expectedCategories map[types.StateChangeCategory]int) {
	actualCategories := make(map[types.StateChangeCategory]int)

	for _, edge := range stateChanges.Edges {
		suite.Require().NotNil(edge.Node, "state change node should not be nil")
		category := edge.Node.GetType()
		actualCategories[category]++
	}

	for category, expectedCount := range expectedCategories {
		actualCount := actualCategories[category]
		suite.Require().Equal(expectedCount, actualCount, "mismatch for category %s", category)
	}
}

func (suite *DataValidationTestSuite) TestPaymentOperationDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating payment operation data...")

	// Find the payment use case
	paymentUseCase := findUseCase(suite, "Stellarclassic/paymentOp")
	suite.Require().NotNil(paymentUseCase, "paymentOp use case not found")
	suite.Require().NotEmpty(paymentUseCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := paymentUseCase.GetTransactionResult.Hash

	// Validate transaction using helper
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validatePaymentOperations(ctx, txHash)
	suite.validatePaymentStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validatePaymentOperations(ctx context.Context, txHash string) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation)
	suite.Require().Equal(types.OperationTypePayment, operation.OperationType, "operation type should be PAYMENT")
}

func (suite *DataValidationTestSuite) validatePaymentStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 2 state changes")

	// Get XLM contract address
	contractID, err := xlmAsset.ContractID(suite.testEnv.NetworkPassphrase)
	suite.Require().NoError(err, "failed to get contract ID")
	xlmContractAddress := strkey.MustEncode(strkey.VersionByteContract, contractID[:])

	// Verify expected state change counts
	assertStateChangeCounts(suite, stateChanges, map[types.StateChangeReason]int{
		types.StateChangeReasonDebit:  1,
		types.StateChangeReasonCredit: 1,
	})

	// Validate each state change
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)

		balanceChange, ok := edge.Node.(*types.StandardBalanceChange)
		suite.Require().True(ok, "state change should be StandardBalanceChange type")

		if edge.Node.GetReason() == types.StateChangeReasonDebit {
			validateBalanceChange(suite, balanceChange, xlmContractAddress, "100000000",
				suite.testEnv.PrimaryAccountKP.Address(), types.StateChangeReasonDebit)
		} else {
			validateBalanceChange(suite, balanceChange, xlmContractAddress, "100000000",
				suite.testEnv.SecondaryAccountKP.Address(), types.StateChangeReasonCredit)
		}
	}
}

func (suite *DataValidationTestSuite) TestSponsoredAccountCreationDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating sponsored account creation operations data...")

	// Find the sponsored account creation use case
	useCase := findUseCase(suite, "Stellarclassic/sponsoredAccountCreationOps")
	suite.Require().NotNil(useCase, "sponsoredAccountCreationOps use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash

	// Validate transaction
	tx := validateTransactionBase(suite, ctx, txHash)

	// Validate operations (BeginSponsoringFutureReserves, CreateAccount, ManageData, EndSponsoringFutureReserves)
	suite.validateSponsoredAccountCreationOperations(ctx, txHash)

	// Validate state changes (9 total)
	suite.validateSponsoredAccountCreationStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateSponsoredAccountCreationOperations(ctx context.Context, txHash string) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 4, "should have exactly 4 operations")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeBeginSponsoringFutureReserves,
		types.OperationTypeCreateAccount,
		types.OperationTypeManageData,
		types.OperationTypeEndSponsoringFutureReserves,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node)
		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateSponsoredAccountCreationStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(20)
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 9, "should have exactly 9 state changes")

	// Verify expected state change counts by category
	assertStateChangeCategories(suite, stateChanges, map[types.StateChangeCategory]int{
		types.StateChangeCategoryBalance:  3,
		types.StateChangeCategoryAccount:  1,
		types.StateChangeCategoryMetadata: 1,
		types.StateChangeCategoryReserves: 4,
	})

	// Verify expected state change counts by reason
	assertStateChangeCounts(suite, stateChanges, map[types.StateChangeReason]int{
		types.StateChangeReasonDebit:     2,
		types.StateChangeReasonCredit:    1,
		types.StateChangeReasonCreate:    1,
		types.StateChangeReasonDataEntry: 1,
		types.StateChangeReasonSponsor:   2,
		types.StateChangeReasonUnsponsor: 2,
	})

	// Validate each state change with specific field validations
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)

		// Validate specific fields based on state change type
		switch sc := edge.Node.(type) {
		case *types.StandardBalanceChange:
			// Validate balance changes (tokenId, amount, account)
			suite.Require().NotEmpty(sc.TokenID, "token ID should not be empty")
			suite.Require().NotEmpty(sc.Amount, "amount should not be empty")
			suite.Require().NotEmpty(sc.GetAccountID(), "account ID should not be empty")

		case *types.AccountChange:
			// Validate account change (tokenId, amount, account, reason should be CREATE)
			validateAccountChange(suite, sc, sc.TokenID, sc.Amount, sc.GetAccountID(), types.StateChangeReasonCreate)

		case *types.MetadataChange:
			// Validate metadata change (keyValue, account, reason should be DATA_ENTRY)
			validateMetadataChange(suite, sc, sc.GetAccountID(), types.StateChangeReasonDataEntry)

		case *types.ReservesChange:
			// Validate reserves change (sponsored/sponsor addresses, account)
			validateReservesChange(suite, sc, sc.GetAccountID(), sc.GetReason())

		default:
			suite.Fail("unexpected state change type: %T", sc)
		}
	}
}

func (suite *DataValidationTestSuite) TestCustomAssetsOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating custom assets operations data...")

	// Find the custom assets use case
	useCase := findUseCase(suite, "Stellarclassic/customAssetsOps")
	suite.Require().NotNil(useCase, "customAssetsOps use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash

	// Validate transaction
	tx := validateTransactionBase(suite, ctx, txHash)

	// Validate operations (8 operations)
	suite.validateCustomAssetsOperations(ctx, txHash)

	// Validate state changes (15+ variable based on trade execution)
	suite.validateCustomAssetsStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateCustomAssetsOperations(ctx context.Context, txHash string) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 8, "should have exactly 8 operations")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeChangeTrust,
		types.OperationTypePayment,
		types.OperationTypeCreatePassiveSellOffer,
		types.OperationTypePathPaymentStrictSend,
		types.OperationTypeManageSellOffer,
		types.OperationTypeManageBuyOffer,
		types.OperationTypePathPaymentStrictReceive,
		types.OperationTypeChangeTrust,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node)
		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateCustomAssetsStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(50)
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")

	// Variable state changes based on trade execution (15+ minimum)
	suite.Require().GreaterOrEqual(len(stateChanges.Edges), 15, "should have at least 15 state changes")

	// Validate each state change with specific field validations
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)

		// Validate specific fields based on state change type
		switch sc := edge.Node.(type) {
		case *types.StandardBalanceChange:
			// Validate balance changes (tokenId, amount, account)
			suite.Require().NotEmpty(sc.TokenID, "token ID should not be empty")
			suite.Require().NotEmpty(sc.Amount, "amount should not be empty")
			suite.Require().NotEmpty(sc.GetAccountID(), "account ID should not be empty")

		case *types.TrustlineChange:
			// Validate trustline changes (limit, account)
			validateTrustlineChange(suite, sc, sc.GetAccountID(), sc.GetReason())

		case *types.FlagsChange:
			// Validate flags changes (flags, account)
			validateFlagsChange(suite, sc, sc.GetAccountID(), sc.GetReason())

		case *types.AccountChange:
			// Validate account changes (tokenId, amount, account)
			suite.Require().NotEmpty(sc.TokenID, "token ID should not be empty")
			suite.Require().NotEmpty(sc.Amount, "amount should not be empty")
			suite.Require().NotEmpty(sc.GetAccountID(), "account ID should not be empty")

		default:
			// Allow other state change types without specific validation
			suite.Require().NotEmpty(sc.GetAccountID(), "account ID should not be empty")
		}
	}
}

func (suite *DataValidationTestSuite) TestAuthRequiredOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating auth required operations data...")

	// Find the auth required use case
	useCase := findUseCase(suite, "Stellarclassic/authRequiredOps")
	suite.Require().NotNil(useCase, "authRequiredOps use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash

	// Validate transaction
	tx := validateTransactionBase(suite, ctx, txHash)

	// Validate operations (8 operations)
	suite.validateAuthRequiredOperations(ctx, txHash)

	// Validate state changes (~18)
	suite.validateAuthRequiredStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateAuthRequiredOperations(ctx context.Context, txHash string) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 8, "should have exactly 8 operations")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeSetOptions,
		types.OperationTypeChangeTrust,
		types.OperationTypeSetTrustLineFlags,
		types.OperationTypePayment,
		types.OperationTypeSetTrustLineFlags,
		types.OperationTypeClawback,
		types.OperationTypeChangeTrust,
		types.OperationTypeSetOptions,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node)
		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateAuthRequiredStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(50)
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")

	// Should have around 18 state changes
	suite.Require().GreaterOrEqual(len(stateChanges.Edges), 15, "should have at least 15 state changes")

	// Validate each state change with specific field validations
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)

		// Validate specific fields based on state change type
		switch sc := edge.Node.(type) {
		case *types.StandardBalanceChange:
			// Validate balance changes (tokenId, amount, account)
			suite.Require().NotEmpty(sc.TokenID, "token ID should not be empty")
			suite.Require().NotEmpty(sc.Amount, "amount should not be empty")
			suite.Require().NotEmpty(sc.GetAccountID(), "account ID should not be empty")

		case *types.TrustlineChange:
			// Validate trustline changes (limit, account)
			validateTrustlineChange(suite, sc, sc.GetAccountID(), sc.GetReason())

		case *types.FlagsChange:
			// Validate flags changes (flags, account)
			validateFlagsChange(suite, sc, sc.GetAccountID(), sc.GetReason())

		case *types.BalanceAuthorizationChange:
			// Validate balance authorization changes (flags, account)
			validateBalanceAuthorizationChange(suite, sc, sc.GetAccountID(), sc.GetReason())

		case *types.SignerChange:
			// Validate signer changes (signer address/weights, account)
			validateSignerChange(suite, sc, sc.GetAccountID(), sc.GetReason())

		case *types.SignerThresholdsChange:
			// Validate signer thresholds changes (thresholds, account)
			validateSignerThresholdsChange(suite, sc, sc.GetAccountID(), sc.GetReason())

		case *types.AccountChange:
			// Validate account changes (tokenId, amount, account)
			suite.Require().NotEmpty(sc.TokenID, "token ID should not be empty")
			suite.Require().NotEmpty(sc.Amount, "amount should not be empty")
			suite.Require().NotEmpty(sc.GetAccountID(), "account ID should not be empty")

		default:
			// Allow other state change types without specific validation
			suite.Require().NotEmpty(sc.GetAccountID(), "account ID should not be empty")
		}
	}
}

func (suite *DataValidationTestSuite) TestAccountMergeOpDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating account merge operation data...")

	// Find the account merge use case
	useCase := findUseCase(suite, "Stellarclassic/accountMergeOp")
	suite.Require().NotNil(useCase, "accountMergeOp use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash

	// Validate transaction
	tx := validateTransactionBase(suite, ctx, txHash)

	// Validate operations (1 operation)
	suite.validateAccountMergeOperations(ctx, txHash)

	// Validate state changes (3 total)
	suite.validateAccountMergeStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateAccountMergeOperations(ctx context.Context, txHash string) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation)
	suite.Require().Equal(types.OperationTypeAccountMerge, operation.OperationType, "operation type should be ACCOUNT_MERGE")
}

func (suite *DataValidationTestSuite) validateAccountMergeStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 3, "should have exactly 3 state changes")

	// Verify expected state change counts by category
	assertStateChangeCategories(suite, stateChanges, map[types.StateChangeCategory]int{
		types.StateChangeCategoryBalance: 2,
		types.StateChangeCategoryAccount: 1,
	})

	// Verify expected state change counts by reason
	assertStateChangeCounts(suite, stateChanges, map[types.StateChangeReason]int{
		types.StateChangeReasonDebit:  1,
		types.StateChangeReasonCredit: 1,
		types.StateChangeReasonMerge:  1,
	})

	// Validate each state change with specific field validations
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)

		// Validate specific fields based on state change type
		switch sc := edge.Node.(type) {
		case *types.StandardBalanceChange:
			// Validate balance changes (tokenId, amount, account)
			suite.Require().NotEmpty(sc.TokenID, "token ID should not be empty")
			suite.Require().NotEmpty(sc.Amount, "amount should not be empty")
			suite.Require().NotEmpty(sc.GetAccountID(), "account ID should not be empty")

		case *types.AccountChange:
			// Validate account merge (tokenId, amount, account, reason should be MERGE)
			validateAccountChange(suite, sc, sc.TokenID, sc.Amount, sc.GetAccountID(), types.StateChangeReasonMerge)

		default:
			suite.Fail("unexpected state change type: %T", sc)
		}
	}
}

func (suite *DataValidationTestSuite) TestInvokeContractOpSorobanAuthDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating invoke contract operation with Soroban auth data...")

	// Find the invoke contract with Soroban auth use case
	useCase := findUseCase(suite, "Soroban/invokeContractOp/SorobanAuth")
	suite.Require().NotNil(useCase, "invokeContractOp/SorobanAuth use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash

	// Validate transaction
	tx := validateTransactionBase(suite, ctx, txHash)

	// Validate operations (1 operation)
	suite.validateInvokeContractOperations(ctx, txHash)

	// Validate state changes (3 total)
	suite.validateInvokeContractStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateInvokeContractOperations(ctx context.Context, txHash string) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation)
	suite.Require().Equal(types.OperationTypeInvokeHostFunction, operation.OperationType, "operation type should be INVOKE_HOST_FUNCTION")
}

func (suite *DataValidationTestSuite) validateInvokeContractStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 3, "should have exactly 3 state changes")

	// Verify expected state change counts by reason (fee debit + transfer debit + transfer credit)
	assertStateChangeCounts(suite, stateChanges, map[types.StateChangeReason]int{
		types.StateChangeReasonDebit:  2,
		types.StateChangeReasonCredit: 1,
	})

	// Validate each state change with specific field validations
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)

		// All state changes should be StandardBalanceChange
		balanceChange, ok := edge.Node.(*types.StandardBalanceChange)
		suite.Require().True(ok, "state change should be StandardBalanceChange type")

		// Validate balance change fields (tokenId, amount, account)
		suite.Require().NotEmpty(balanceChange.TokenID, "token ID should not be empty")
		suite.Require().NotEmpty(balanceChange.Amount, "amount should not be empty")
		suite.Require().NotEmpty(balanceChange.GetAccountID(), "account ID should not be empty")
	}
}

func (suite *DataValidationTestSuite) TestInvokeContractOpSourceAccountAuthDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating invoke contract operation with source account auth data...")

	// Find the invoke contract with source account auth use case
	useCase := findUseCase(suite, "Soroban/invokeContractOp/SourceAccountAuth")
	suite.Require().NotNil(useCase, "invokeContractOp/SourceAccountAuth use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash

	// Validate transaction
	tx := validateTransactionBase(suite, ctx, txHash)

	// Validate operations (1 operation)
	suite.validateInvokeContractOperations(ctx, txHash)

	// Validate state changes (3 total)
	suite.validateInvokeContractStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}
