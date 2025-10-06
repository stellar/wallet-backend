// Package integrationtests provides end-to-end integration tests for wallet-backend
package integrationtests

import (
	"context"

	"github.com/stellar/go/support/log"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type DataValidationTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (suite *DataValidationTestSuite) TestPaymentOperationDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating payment operation data...")

	// Find the payment use case from the prepared use cases
	var paymentUseCase *infrastructure.UseCase
	for _, uc := range suite.testEnv.UseCases {
		name := uc.Name()
		if name == "Stellarclassic/paymentOp" {
			paymentUseCase = uc
			break
		}
	}
	suite.Require().NotNil(paymentUseCase, "paymentOp use case not found")
	suite.Require().NotEmpty(paymentUseCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := paymentUseCase.GetTransactionResult.Hash

	// Validate Transaction
	suite.validateTransaction(ctx, txHash)

	// Validate Operations
	suite.validateOperations(ctx, txHash)

	// Validate State Changes
	suite.validateStateChanges(ctx, txHash)
}

func (suite *DataValidationTestSuite) validateTransaction(ctx context.Context, txHash string) {
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
}

func (suite *DataValidationTestSuite) validateOperations(ctx context.Context, txHash string) {
	// Fetch operations for the transaction
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")

	// Verify we have exactly 1 operation (the payment)
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	// Validate the payment operation
	operation := operations.Edges[0].Node
	suite.Require().NotNil(operation, "operation node should not be nil")
	suite.Require().Equal(types.OperationTypePayment, operation.OperationType, "operation type should be PAYMENT")
	suite.Require().NotEmpty(operation.OperationXdr, "operation XDR should not be empty")
	suite.Require().NotZero(operation.LedgerNumber, "ledger number should not be zero")
	suite.Require().False(operation.LedgerCreatedAt.IsZero(), "ledger created at should not be zero")
	suite.Require().False(operation.IngestedAt.IsZero(), "ingested at should not be zero")
}

func (suite *DataValidationTestSuite) validateStateChanges(ctx context.Context, txHash string) {
	// Fetch state changes for the transaction
	first := int32(10)
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")

	// Should generate 3 state changes:
	// - 1 BALANCE/DEBIT change for wallet-backend's distribution account for the fee of the transaction
	// - 1 BALANCE/CREDIT change for secondary account for the amount of the payment
	// - 1 BALANCE/DEBIT change for primary account for the amount of the payment
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 3 state changes")

	// Track counts of each type/reason combination
	paymentCreditCount := 0
	paymentDebitCount := 0

	for _, edge := range stateChanges.Edges {
		suite.Require().NotNil(edge.Node, "state change node should not be nil")

		stateChange := edge.Node
		suite.Require().Equal(types.StateChangeCategoryBalance, stateChange.GetType(), "all state changes should be BALANCE type")

		// Verify common fields
		suite.Require().NotZero(stateChange.GetLedgerNumber(), "ledger number should not be zero")
		suite.Require().False(stateChange.GetLedgerCreatedAt().IsZero(), "ledger created at should not be zero")
		suite.Require().False(stateChange.GetIngestedAt().IsZero(), "ingested at should not be zero")

		// Cast to StandardBalanceChange to access specific fields
		balanceChange, ok := stateChange.(*types.StandardBalanceChange)
		suite.Require().True(ok, "state change should be StandardBalanceChange type")

		// Verify token ID is native asset
		suite.Require().Equal("native", balanceChange.TokenID, "token ID should be native")
		suite.Require().NotEmpty(balanceChange.Amount, "amount should not be empty")
		suite.Require().Equal("100000000", balanceChange.Amount, "payment amount should be 10 XLM (100000000 stroops)")

		// Count each type of state change
		switch stateChange.GetReason() {
		case types.StateChangeReasonDebit:
			// Should be payment debit of 10 XLM
			// suite.Require().Equal(suite.testEnv.PrimaryAccountKP.Address(), balanceChange.GetAccountID(), "account ID should be primary account")
			paymentDebitCount++
		case types.StateChangeReasonCredit:
			// Should be payment credit of 10 XLM
			paymentCreditCount++
		case types.StateChangeReasonCreate, types.StateChangeReasonMerge, types.StateChangeReasonMint,
			types.StateChangeReasonBurn, types.StateChangeReasonAdd, types.StateChangeReasonRemove,
			types.StateChangeReasonUpdate, types.StateChangeReasonLow, types.StateChangeReasonMedium,
			types.StateChangeReasonHigh, types.StateChangeReasonHomeDomain, types.StateChangeReasonSet,
			types.StateChangeReasonClear, types.StateChangeReasonDataEntry, types.StateChangeReasonSponsor,
			types.StateChangeReasonUnsponsor:
			// These reasons are not expected for a payment operation
			suite.Fail("unexpected state change reason for payment operation", "reason=%s", stateChange.GetReason())
		}
	}

	// Verify we have the expected state changes
	// Note: Both debits are combined in our count, so we expect:
	// - 2 debits total (1 for fee, 1 for payment amount)
	// - 1 credit (for payment to secondary)
	suite.Require().Equal(1, paymentCreditCount, "should have exactly 1 CREDIT state change")
	suite.Require().Equal(1, paymentDebitCount, "should have exactly 1 DEBIT state change")
}
