// Package integrationtests provides end-to-end integration tests for wallet-backend
package integrationtests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
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
	pool    pond.Pool
}

// SetupSuite initializes the pool for parallel test execution
func (suite *DataValidationTestSuite) SetupSuite() {
	suite.pool = pond.NewPool(10)
}

// TearDownSuite cleans up the pool after all tests complete
func (suite *DataValidationTestSuite) TearDownSuite() {
	suite.pool.StopAndWait()
}

// getAssetContractAddress computes the contract address for a given asset
func (suite *DataValidationTestSuite) getAssetContractAddress(asset xdr.Asset) string {
	contractID, err := asset.ContractID(suite.testEnv.NetworkPassphrase)
	suite.Require().NoError(err, "failed to get contract ID")
	return strkey.MustEncode(strkey.VersionByteContract, contractID[:])
}

// stateChangeQuery defines a query for fetching state changes
type stateChangeQuery struct {
	name     string
	account  string
	txHash   *string
	category *string
	reason   *string
}

// fetchStateChangesInParallel fetches multiple state changes in parallel using pond worker pool
func (suite *DataValidationTestSuite) fetchStateChangesInParallel(
	ctx context.Context,
	queries []stateChangeQuery,
	first *int32,
) map[string]*types.StateChangeConnection {
	results := make(map[string]*types.StateChangeConnection)
	resultsMu := sync.Mutex{}
	group := suite.pool.NewGroupContext(ctx)
	var errs []error
	errMu := sync.Mutex{}

	for _, q := range queries {
		query := q // capture variable
		group.Submit(func() {
			sc, err := suite.testEnv.WBClient.GetAccountStateChanges(
				ctx, query.account, query.txHash, nil, query.category, query.reason, first, nil, nil, nil)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", query.name, err))
				errMu.Unlock()
				return
			}
			resultsMu.Lock()
			results[query.name] = sc
			resultsMu.Unlock()
		})
	}

	suite.Require().NoError(group.Wait(), "waiting for parallel state change fetches")
	if len(errs) > 0 {
		suite.Require().Fail("errors fetching state changes", errors.Join(errs...))
	}
	return results
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
func validateOperationBase(suite *DataValidationTestSuite, op *types.Operation, expectedLedgerNumber int64, expectedOperationType types.OperationType) {
	suite.Require().NotNil(op, "operation should not be nil")
	suite.Require().Equal(expectedOperationType, op.OperationType, "operation type mismatch")
	suite.Require().NotEmpty(op.OperationXdr, "operation XDR should not be empty")
	suite.Require().Equal(expectedLedgerNumber, int64(op.LedgerNumber), "ledger number mismatch")
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
func validateAccountChange(suite *DataValidationTestSuite, ac *types.AccountChange, expectedAccount, expectedFunderAddress string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(ac, "account change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryAccount, ac.GetType(), "should be ACCOUNT type")
	suite.Require().Equal(expectedReason, ac.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedAccount, ac.GetAccountID(), "account ID mismatch")
	suite.Require().Equal(expectedFunderAddress, *ac.FunderAddress, "funder address mismatch")
}

// validateSignerChange validates a signer state change
func validateSignerChange(suite *DataValidationTestSuite, sc *types.SignerChange, expectedAccount string, expectedSignerAddress string, expectedSignerWeights int32, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(sc, "signer change should not be nil")
	suite.Require().Equal(types.StateChangeCategorySigner, sc.GetType(), "should be SIGNER type")
	suite.Require().Equal(expectedReason, sc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedAccount, sc.GetAccountID(), "account ID mismatch")
	suite.Require().NotNil(sc.SignerAddress, "signer address should not be nil")
	suite.Require().Equal(expectedSignerAddress, *sc.SignerAddress, "signer address mismatch")

	// Decode the key value
	suite.Require().NotNil(sc.SignerWeights, "signer weights should not be nil")
	var result map[string]int32
	err := json.Unmarshal([]byte(*sc.SignerWeights), &result)
	suite.Require().NoError(err, "failed to unmarshal signer weights", sc.SignerWeights)
	value, ok := result["new"]
	suite.Require().True(ok, "key should exist in the result")
	suite.Require().Equal(expectedSignerWeights, value, "signer weights do not match in the result")
}

// validateMetadataChange validates a metadata state change
func validateMetadataChange(suite *DataValidationTestSuite, mc *types.MetadataChange, expectedAccount string, expectedReason types.StateChangeReason, expectedKey string, expectedValue string) {
	suite.Require().NotNil(mc, "metadata change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryMetadata, mc.GetType(), "should be METADATA type")
	suite.Require().Equal(expectedReason, mc.GetReason(), "reason mismatch")
	suite.Require().NotEmpty(mc.KeyValue, "key value should not be empty")
	suite.Require().Equal(expectedAccount, mc.GetAccountID(), "account ID mismatch")

	// Decode the key value
	var result map[string]map[string]string
	err := json.Unmarshal([]byte(mc.KeyValue), &result)
	suite.Require().NoError(err, "failed to unmarshal metadata change key value", mc.KeyValue)
	value, ok := result[expectedKey]["new"]
	suite.Require().True(ok, "key should exist in the result")
	valueDecoded, err := base64.StdEncoding.DecodeString(value)
	suite.Require().NoError(err, "failed to decode value", value)
	suite.Require().Equal(expectedValue, string(valueDecoded), "value does not match in the result")
}

// validateReservesChange validates a reserves state change
func validateReservesSponsorshipChangeForSponsoredAccount(suite *DataValidationTestSuite, rc *types.ReservesChange, expectedAccount string, 
	expectedReason types.StateChangeReason, expectedSponsorAddress string, expectedKey string, expectedValue string) {
	suite.Require().NotNil(rc, "reserves sponsorship change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryReserves, rc.GetType(), "should be RESERVES type")
	suite.Require().Equal(expectedReason, rc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedAccount, rc.GetAccountID(), "account ID mismatch")
	suite.Require().Equal(expectedSponsorAddress, *rc.SponsorAddress, "sponsor address mismatch")

	// Decode the key value
	if expectedKey != "" && expectedValue != "" {
		suite.Require().NotNil(rc.KeyValue, "key value should not be nil")
		var result map[string]string
		err := json.Unmarshal([]byte(*rc.KeyValue), &result)
		suite.Require().NoError(err, "failed to unmarshal key value", rc.KeyValue)
		value, ok := result[expectedKey]
		suite.Require().True(ok, "key should exist in the result")
		suite.Require().Equal(expectedValue, value, "value does not match in the result")
	}
}

func validateReservesSponsorshipChangeForSponsoringAccount(suite *DataValidationTestSuite, rc *types.ReservesChange, expectedAccount string,
	expectedReason types.StateChangeReason, expectedSponsoredAddress string, expectedKey string, expectedValue string,
) {
	suite.Require().NotNil(rc, "reserves sponsorship change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryReserves, rc.GetType(), "should be RESERVES type")
	suite.Require().Equal(expectedReason, rc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedAccount, rc.GetAccountID(), "account ID mismatch")
	if expectedSponsoredAddress != "" {
		suite.Require().Equal(expectedSponsoredAddress, *rc.SponsoredAddress, "sponsored address mismatch")
	}

	// Decode the key value
	if expectedKey != "" && expectedValue != "" {
		suite.Require().NotNil(rc.KeyValue, "key value should not be nil")
		var result map[string]string
		err := json.Unmarshal([]byte(*rc.KeyValue), &result)
		suite.Require().NoError(err, "failed to unmarshal key value", rc.KeyValue)
		value, ok := result[expectedKey]
		suite.Require().True(ok, "key should exist in the result")
		suite.Require().Equal(expectedValue, value, "value does not match in the result")
	}
}

// sumAmounts aggregates amounts from balance changes for a specific token and returns the total as int64
func sumAmounts(suite *DataValidationTestSuite, sc *types.StateChangeConnection, tokenID string) int64 {
	var total int64
	for _, edge := range sc.Edges {
		balanceChange, ok := edge.Node.(*types.StandardBalanceChange)
		suite.Require().True(ok, "state change should be StandardBalanceChange type")

		// Skip if tokenID doesn't match (filter by token)
		if balanceChange.TokenID != tokenID {
			continue
		}

		// Parse amount string to int64
		var amount int64
		_, err := fmt.Sscanf(balanceChange.Amount, "%d", &amount)
		suite.Require().NoError(err, "failed to parse amount: %s", balanceChange.Amount)
		total += amount
	}
	return total
}

// validateTrustlineChangeDetailed validates a trustline state change with detailed checks
func validateTrustlineChange(suite *DataValidationTestSuite, tc *types.TrustlineChange, expectedAccount string, expectedTokenID string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(tc, "trustline change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryTrustline, tc.GetType(), "should be TRUSTLINE type")
	suite.Require().Equal(expectedReason, tc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedAccount, tc.GetAccountID(), "account ID mismatch")
	suite.Require().Equal(expectedTokenID, tc.TokenID, "token ID mismatch")
	if expectedReason == types.StateChangeReasonAdd {
		suite.Require().NotNil(tc.Limit, "limit should not be nil for ADD")
		suite.Require().NotEmpty(*tc.Limit, "limit should not be empty for ADD")
	}
}

// validateBalanceAuthorizationChangeDetailed validates a balance authorization state change
func validateBalanceAuthorizationChange(suite *DataValidationTestSuite, bac *types.BalanceAuthorizationChange, expectedAccount string, expectedReason types.StateChangeReason, expectedFlags []string) {
	suite.Require().NotNil(bac, "balance authorization change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryBalanceAuthorization, bac.GetType(), "should be BALANCE_AUTHORIZATION type")
	suite.Require().Equal(expectedReason, bac.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedAccount, bac.GetAccountID(), "account ID mismatch")
	suite.Require().Equal(len(expectedFlags), len(bac.Flags), "flags count mismatch")
	for _, expectedFlag := range expectedFlags {
		suite.Require().Contains(bac.Flags, expectedFlag, "expected flag not found: %s", expectedFlag)
	}
}

// validateFlagsChange validates a flags state change
func validateFlagsChange(suite *DataValidationTestSuite, fc *types.FlagsChange, expectedAccount string, expectedReason types.StateChangeReason, expectedFlags []string) {
	suite.Require().NotNil(fc, "flags change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryFlags, fc.GetType(), "should be FLAGS type")
	suite.Require().Equal(expectedReason, fc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedAccount, fc.GetAccountID(), "account ID mismatch")
	suite.Require().Equal(len(expectedFlags), len(fc.Flags), "flags count mismatch")
	for _, expectedFlag := range expectedFlags {
		suite.Require().Contains(fc.Flags, expectedFlag, "expected flag not found: %s", expectedFlag)
	}
}

func (suite *DataValidationTestSuite) TestPaymentOperationDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating payment operation data...")

	// Find the payment use case
	paymentUseCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/paymentOp")
	suite.Require().NotNil(paymentUseCase, "paymentOp use case not found")
	suite.Require().NotEmpty(paymentUseCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := paymentUseCase.GetTransactionResult.Hash

	// Validate transaction using helper
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validatePaymentOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validatePaymentStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validatePaymentOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation, ledgerNumber, types.OperationTypePayment)
	suite.Require().Equal(types.OperationTypePayment, operation.OperationType, "operation type should be PAYMENT")
}

func (suite *DataValidationTestSuite) validatePaymentStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	balanceCategory := "BALANCE"
	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	secondaryAccount := suite.testEnv.SecondaryAccountKP.Address()

	// Fetch balance changes for both accounts in parallel
	paymentQueries := []stateChangeQuery{
		{name: "primaryBalanceChange", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: nil},
		{name: "secondaryBalanceChange", account: secondaryAccount, txHash: &txHash, category: &balanceCategory, reason: nil},
	}
	paymentResults := suite.fetchStateChangesInParallel(ctx, paymentQueries, &first)

	// Extract results
	primaryStateChanges := paymentResults["primaryBalanceChange"]
	secondaryStateChanges := paymentResults["secondaryBalanceChange"]

	// Validate results are not nil
	suite.Require().NotNil(primaryStateChanges, "primary state changes should not be nil")
	suite.Require().NotNil(secondaryStateChanges, "secondary state changes should not be nil")

	// 1 DEBIT change for primary account
	suite.Require().Len(primaryStateChanges.Edges, 1, "should have exactly 1 state change for primary account")
	sc := primaryStateChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, sc, ledgerNumber)
	validateBalanceChange(suite, sc, xlmContractAddress, "100000000", primaryAccount, types.StateChangeReasonDebit)

	// 1 CREDIT change for secondary account
	suite.Require().Len(secondaryStateChanges.Edges, 1, "should have exactly 1 state change for secondary account")
	sc = secondaryStateChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, sc, ledgerNumber)
	validateBalanceChange(suite, sc, xlmContractAddress, "100000000", secondaryAccount, types.StateChangeReasonCredit)

	// Only 2 state changes for this transaction
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 2 state changes")
}

func (suite *DataValidationTestSuite) TestSponsoredAccountCreationDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating sponsored account creation operations data...")

	// Find the sponsored account creation use case
	useCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/sponsoredAccountCreationOps")
	suite.Require().NotNil(useCase, "sponsoredAccountCreationOps use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validateSponsoredAccountCreationOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validateSponsoredAccountCreationStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateSponsoredAccountCreationOperations(ctx context.Context, txHash string, ledgerNumber int64) {
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
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
	}
}

func (suite *DataValidationTestSuite) validateSponsoredAccountCreationStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(20)
	balanceCategory := "BALANCE"
	accountCategory := "ACCOUNT"
	metadataCategory := "METADATA"
	reservesCategory := "RESERVES"
	sponsorReason := "SPONSOR"
	signerCategory := "SIGNER"
	addReason := "ADD"
	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	sponsoredNewAccount := suite.testEnv.SponsoredNewAccountKP.Address()

	// Fetch all state changes in parallel
	sponsorshipQueries := []stateChangeQuery{
		{name: "primaryBalanceChange", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: nil},
		{name: "sponsoredBalanceChange", account: sponsoredNewAccount, txHash: &txHash, category: &balanceCategory, reason: nil},
		{name: "sponsoredAccountChange", account: sponsoredNewAccount, txHash: &txHash, category: &accountCategory, reason: nil},
		{name: "primaryMetadataChange", account: primaryAccount, txHash: &txHash, category: &metadataCategory, reason: nil},
		{name: "sponsoredReservesChange", account: sponsoredNewAccount, txHash: &txHash, category: &reservesCategory, reason: &sponsorReason},
		{name: "primaryReservesChange", account: primaryAccount, txHash: &txHash, category: &reservesCategory, reason: &sponsorReason},
		{name: "sponsoredSignerChange", account: sponsoredNewAccount, txHash: &txHash, category: &signerCategory, reason: &addReason},
	}
	sponsorshipResults := suite.fetchStateChangesInParallel(ctx, sponsorshipQueries, &first)

	// Extract and validate results
	primaryBalanceChanges := sponsorshipResults["primaryBalanceChange"]
	sponsoredBalanceChanges := sponsorshipResults["sponsoredBalanceChange"]
	sponsoredAccountChanges := sponsorshipResults["sponsoredAccountChange"]
	primaryMetadataChanges := sponsorshipResults["primaryMetadataChange"]
	sponsoredReservesChanges := sponsorshipResults["sponsoredReservesChange"]
	primaryReservesChanges := sponsorshipResults["primaryReservesChange"]
	sponsoredSignerChanges := sponsorshipResults["sponsoredSignerChange"]

	// Validate all results are not nil
	suite.Require().NotNil(primaryBalanceChanges, "primary balance changes should not be nil")
	suite.Require().NotNil(sponsoredBalanceChanges, "sponsored balance changes should not be nil")
	suite.Require().NotNil(sponsoredAccountChanges, "sponsored account changes should not be nil")
	suite.Require().NotNil(primaryMetadataChanges, "primary metadata changes should not be nil")
	suite.Require().NotNil(sponsoredReservesChanges, "sponsored reserves changes should not be nil")
	suite.Require().NotNil(primaryReservesChanges, "primary reserves changes should not be nil")
	suite.Require().NotNil(sponsoredSignerChanges, "sponsored signer changes should not be nil")

	// 1 BALANCE/DEBIT change for primary account (sending starting balance)
	suite.Require().Len(primaryBalanceChanges.Edges, 1, "should have exactly 1 BALANCE/DEBIT balance change for primary account")
	balanceChange := primaryBalanceChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, balanceChange, ledgerNumber)
	validateBalanceChange(suite, balanceChange, xlmContractAddress, "50000000", primaryAccount, types.StateChangeReasonDebit)

	// 1 BALANCE/CREDIT change for sponsored account (receiving starting balance)
	suite.Require().Len(sponsoredBalanceChanges.Edges, 1, "should have exactly 1 BALANCE/CREDIT balance change for sponsored account")
	balanceChange = sponsoredBalanceChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, balanceChange, ledgerNumber)
	validateBalanceChange(suite, balanceChange, xlmContractAddress, "50000000", sponsoredNewAccount, types.StateChangeReasonCredit)

	// 1 ACCOUNT/CREATE account change for sponsored account
	suite.Require().Len(sponsoredAccountChanges.Edges, 1, "should have exactly 1 ACCOUNT/CREATE account change")
	accountChange := sponsoredAccountChanges.Edges[0].Node.(*types.AccountChange)
	validateStateChangeBase(suite, accountChange, ledgerNumber)
	validateAccountChange(suite, accountChange, sponsoredNewAccount, primaryAccount, types.StateChangeReasonCreate)

	// 1 METADATA/DATA_ENTRY metadata change for primary account
	suite.Require().Len(primaryMetadataChanges.Edges, 1, "should have exactly 1 METADATA/DATA_ENTRY metadata change for primary account")
	metadataChange := primaryMetadataChanges.Edges[0].Node.(*types.MetadataChange)
	validateStateChangeBase(suite, metadataChange, ledgerNumber)
	validateMetadataChange(suite, metadataChange, primaryAccount, types.StateChangeReasonDataEntry, "foo", "bar")

	// 1 RESERVES/SPONSOR change for sponsored account - sponsorship begin
	suite.Require().Len(sponsoredReservesChanges.Edges, 1, "should have exactly 1 RESERVES/SPONSOR reserves change for sponsored account")
	reserveChange := sponsoredReservesChanges.Edges[0].Node.(*types.ReservesChange)
	validateStateChangeBase(suite, reserveChange, ledgerNumber)
	validateReservesSponsorshipChangeForSponsoredAccount(suite, reserveChange, sponsoredNewAccount, types.StateChangeReasonSponsor, primaryAccount, "", "")

	// 1 RESERVES/SPONSOR change for sponsoring account - sponsorship begin
	suite.Require().Len(primaryReservesChanges.Edges, 1, "should have exactly 1 RESERVES/SPONSOR reserves change for sponsoring account")
	reserveChange = primaryReservesChanges.Edges[0].Node.(*types.ReservesChange)
	validateStateChangeBase(suite, reserveChange, ledgerNumber)
	validateReservesSponsorshipChangeForSponsoringAccount(suite, reserveChange, primaryAccount, types.StateChangeReasonSponsor, sponsoredNewAccount, "", "")

	// 1 SIGNER/ADD change for sponsored account with default signer weight = 1
	suite.Require().Len(sponsoredSignerChanges.Edges, 1, "should have exactly 1 SIGNER/CREATE signer change for sponsored account")
	signerChange := sponsoredSignerChanges.Edges[0].Node.(*types.SignerChange)
	validateStateChangeBase(suite, signerChange, ledgerNumber)
	validateSignerChange(suite, signerChange, sponsoredNewAccount, sponsoredNewAccount, 1, types.StateChangeReasonAdd)

	// Verify total count of state changes for this transaction
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 7, "should have exactly 9 total state changes")
}

func (suite *DataValidationTestSuite) TestCustomAssetsOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating custom assets operations data...")

	// Find the custom assets use case
	useCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/customAssetsOps")
	suite.Require().NotNil(useCase, "customAssetsOps use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validateCustomAssetsOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validateCustomAssetsStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateCustomAssetsOperations(ctx context.Context, txHash string, ledgerNumber int64) {
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
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateCustomAssetsStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(50)

	// Setup: Compute expected values from fixtures
	test2Asset := xdr.MustNewCreditAsset("TEST2", suite.testEnv.PrimaryAccountKP.Address())
	test2ContractAddress := suite.getAssetContractAddress(test2Asset)
	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	secondaryAccount := suite.testEnv.SecondaryAccountKP.Address()

	// Define filter constants
	balanceCategory := "BALANCE"
	trustlineCategory := "TRUSTLINE"
	balanceAuthCategory := "BALANCE_AUTHORIZATION"
	mintReason := string(types.StateChangeReasonMint)
	burnReason := string(types.StateChangeReasonBurn)
	creditReason := string(types.StateChangeReasonCredit)
	debitReason := string(types.StateChangeReasonDebit)
	setReason := string(types.StateChangeReasonSet)

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	log.Ctx(ctx).Info("🔍 Validating total state change count...")
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 25, "should have exactly 25 state changes")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}

	// 2. CONSERVATION LAW VALIDATIONS
	// Fetch MINT/BURN/CREDIT/DEBIT changes in parallel
	conservationQueries := []stateChangeQuery{
		{name: "mintChanges", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &mintReason},
		{name: "burnChanges", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &burnReason},
		{name: "creditChanges", account: secondaryAccount, txHash: &txHash, category: &balanceCategory, reason: &creditReason},
		{name: "debitChanges", account: secondaryAccount, txHash: &txHash, category: &balanceCategory, reason: &debitReason},
	}
	conservationResults := suite.fetchStateChangesInParallel(ctx, conservationQueries, &first)

	// Extract results
	mintChanges := conservationResults["mintChanges"]
	burnChanges := conservationResults["burnChanges"]
	creditChanges := conservationResults["creditChanges"]
	debitChanges := conservationResults["debitChanges"]

	// Validate results are not nil
	suite.Require().NotNil(mintChanges, "MINT changes should not be nil")
	suite.Require().NotNil(burnChanges, "BURN changes should not be nil")
	suite.Require().NotNil(creditChanges, "CREDIT changes should not be nil")
	suite.Require().NotNil(debitChanges, "DEBIT changes should not be nil")

	// 2a. Primary Account: MINT = BURN
	totalMint := sumAmounts(suite, mintChanges, test2ContractAddress)
	totalBurn := sumAmounts(suite, burnChanges, test2ContractAddress)
	suite.Require().Equal(totalMint, totalBurn, "Primary account: MINT should equal BURN for TEST2")

	// 2b. Secondary Account: CREDIT = DEBIT for TEST2
	totalCredit := sumAmounts(suite, creditChanges, test2ContractAddress)
	totalDebit := sumAmounts(suite, debitChanges, test2ContractAddress)
	suite.Require().Equal(totalCredit, totalDebit, "Secondary account: CREDIT should equal DEBIT for TEST2")

	// 3. CATEGORY-BASED VALIDATIONS
	// Fetch TRUSTLINE and BALANCE_AUTHORIZATION changes in parallel
	categoryQueries := []stateChangeQuery{
		{name: "trustlineChanges", account: secondaryAccount, txHash: &txHash, category: &trustlineCategory, reason: nil},
		{name: "authChanges", account: secondaryAccount, txHash: &txHash, category: &balanceAuthCategory, reason: &setReason},
	}
	categoryResults := suite.fetchStateChangesInParallel(ctx, categoryQueries, &first)

	// Extract results
	trustlineChanges := categoryResults["trustlineChanges"]
	authChanges := categoryResults["authChanges"]

	// Validate results are not nil
	suite.Require().NotNil(trustlineChanges, "trustline changes should not be nil")
	suite.Require().NotNil(authChanges, "balance authorization changes should not be nil")

	// 3a. TRUSTLINE Changes: Secondary should have exactly 2 (ADD and REMOVE)
	suite.Require().Len(trustlineChanges.Edges, 2, "should have exactly 2 trustline changes (ADD and REMOVE)")

	// Validate ADD and REMOVE trustline changes
	foundAdd := false
	foundRemove := false
	for _, edge := range trustlineChanges.Edges {
		tc := edge.Node.(*types.TrustlineChange)
		validateStateChangeBase(suite, tc, ledgerNumber)
		suite.Require().Equal(test2ContractAddress, tc.TokenID, "trustline token ID should be TEST2")

		if tc.GetReason() == types.StateChangeReasonAdd {
			validateTrustlineChange(suite, tc, secondaryAccount, test2ContractAddress, types.StateChangeReasonAdd)
			foundAdd = true
		} else if tc.GetReason() == types.StateChangeReasonRemove {
			validateTrustlineChange(suite, tc, secondaryAccount, test2ContractAddress, types.StateChangeReasonRemove)
			foundRemove = true
		}
	}
	suite.Require().True(foundAdd, "should have ADD trustline change")
	suite.Require().True(foundRemove, "should have REMOVE trustline change")

	// 3b. BALANCE_AUTHORIZATION Changes: Secondary should have exactly 1 (SET with empty flags)
	suite.Require().Len(authChanges.Edges, 1, "should have exactly 1 BALANCE_AUTHORIZATION/SET change")
	authChange := authChanges.Edges[0].Node.(*types.BalanceAuthorizationChange)
	validateStateChangeBase(suite, authChange, ledgerNumber)
	validateBalanceAuthorizationChange(suite, authChange, secondaryAccount, types.StateChangeReasonSet, []string{"authorized"})

	// 4. SPECIFIC BALANCE CHANGE VALIDATIONS
	// 4a. Validate MINT changes have correct token ID and account
	for _, edge := range mintChanges.Edges {
		bc := edge.Node.(*types.StandardBalanceChange)
		suite.Require().Equal(test2ContractAddress, bc.TokenID, "MINT token should be TEST2")
		suite.Require().Equal(primaryAccount, bc.GetAccountID(), "MINT account should be Primary")
		suite.Require().NotEmpty(bc.Amount, "MINT amount should not be empty")
	}

	// 4b. Validate BURN changes have correct token ID and account
	for _, edge := range burnChanges.Edges {
		bc := edge.Node.(*types.StandardBalanceChange)
		suite.Require().Equal(test2ContractAddress, bc.TokenID, "BURN token should be TEST2")
		suite.Require().Equal(primaryAccount, bc.GetAccountID(), "BURN account should be Primary")
		suite.Require().NotEmpty(bc.Amount, "BURN amount should not be empty")
	}

	// 4c. Validate CREDIT changes have correct token ID and account
	tokenSet := set.NewSet(test2ContractAddress, xlmContractAddress)
	for _, edge := range creditChanges.Edges {
		bc := edge.Node.(*types.StandardBalanceChange)
		suite.Require().True(tokenSet.Contains(bc.TokenID), "CREDIT token should be TEST2 or XLM")
		suite.Require().Equal(secondaryAccount, bc.GetAccountID(), "CREDIT account should be Secondary")
		suite.Require().NotEmpty(bc.Amount, "CREDIT amount should not be empty")
	}

	// 4d. Validate DEBIT changes have correct token ID and account
	for _, edge := range debitChanges.Edges {
		bc := edge.Node.(*types.StandardBalanceChange)
		suite.Require().True(tokenSet.Contains(bc.TokenID), "DEBIT token should be TEST2 or XLM")
		suite.Require().Equal(secondaryAccount, bc.GetAccountID(), "DEBIT account should be Secondary")
		suite.Require().NotEmpty(bc.Amount, "DEBIT amount should not be empty")
	}
}

func (suite *DataValidationTestSuite) TestAuthRequiredOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating auth-required operations data...")

	// Transaction 1: Issuer Setup - Find the issuer setup use case
	issuerSetupUseCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/authRequiredIssuerSetupOps")
	suite.Require().NotNil(issuerSetupUseCase, "authRequiredIssuerSetupOps use case not found")
	suite.Require().NotEmpty(issuerSetupUseCase.GetTransactionResult.Hash, "issuer setup transaction hash should not be empty")

	issuerSetupTxHash := issuerSetupUseCase.GetTransactionResult.Hash
	issuerSetupTx := validateTransactionBase(suite, ctx, issuerSetupTxHash)
	suite.validateAuthRequiredIssuerSetupOperations(ctx, issuerSetupTxHash, int64(issuerSetupTx.LedgerNumber))
	suite.validateAuthRequiredIssuerSetupStateChanges(ctx, issuerSetupTxHash, int64(issuerSetupTx.LedgerNumber))

	// Transaction 2: Asset Operations - Find the asset operations use case
	assetOpsUseCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/authRequiredAssetOps")
	suite.Require().NotNil(assetOpsUseCase, "authRequiredAssetOps use case not found")
	suite.Require().NotEmpty(assetOpsUseCase.GetTransactionResult.Hash, "asset ops transaction hash should not be empty")

	assetOpsTxHash := assetOpsUseCase.GetTransactionResult.Hash
	assetOpsTx := validateTransactionBase(suite, ctx, assetOpsTxHash)
	suite.validateAuthRequiredAssetOperations(ctx, assetOpsTxHash, int64(assetOpsTx.LedgerNumber))
	suite.validateAuthRequiredAssetStateChanges(ctx, assetOpsTxHash, int64(assetOpsTx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateAuthRequiredIssuerSetupOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeSetOptions,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateAuthRequiredAssetOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 6, "should have exactly 6 operations")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeChangeTrust,
		types.OperationTypeSetTrustLineFlags,
		types.OperationTypePayment,
		types.OperationTypeSetTrustLineFlags,
		types.OperationTypeClawback,
		types.OperationTypeChangeTrust,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateAuthRequiredIssuerSetupStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)

	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()

	// Define filter constants
	flagsCategory := "FLAGS"
	setReason := string(types.StateChangeReasonSet)

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 1, "should have exactly 1 state change")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}

	// 2. FETCH FLAGS/SET STATE CHANGE
	issuerSetupQueries := []stateChangeQuery{
		{name: "flagsSetPrimary", account: primaryAccount, txHash: &txHash, category: &flagsCategory, reason: &setReason},
	}
	issuerSetupResults := suite.fetchStateChangesInParallel(ctx, issuerSetupQueries, &first)

	// Extract results
	flagsSetPrimary := issuerSetupResults["flagsSetPrimary"]

	// Validate results are not nil
	suite.Require().NotNil(flagsSetPrimary, "FLAGS/SET for primary should not be nil")

	// 3. FLAGS STATE CHANGES VALIDATION FOR PRIMARY ACCOUNT
	suite.Require().Len(flagsSetPrimary.Edges, 1, "should have exactly 1 FLAGS/SET change for primary")

	expectedFlags := []string{"auth_required", "auth_revocable", "auth_clawback_enabled"}
	flagsSetChange := flagsSetPrimary.Edges[0].Node.(*types.FlagsChange)
	validateStateChangeBase(suite, flagsSetChange, ledgerNumber)
	validateFlagsChange(suite, flagsSetChange, primaryAccount, types.StateChangeReasonSet, expectedFlags)
}

func (suite *DataValidationTestSuite) validateAuthRequiredAssetStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(15)

	// Setup: Compute expected values from fixtures
	test1Asset := xdr.MustNewCreditAsset("TEST1", suite.testEnv.PrimaryAccountKP.Address())
	test1ContractAddress := suite.getAssetContractAddress(test1Asset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	secondaryAccount := suite.testEnv.SecondaryAccountKP.Address()

	// Define filter constants
	balanceCategory := "BALANCE"
	trustlineCategory := "TRUSTLINE"
	balanceAuthCategory := "BALANCE_AUTHORIZATION"
	setReason := string(types.StateChangeReasonSet)
	clearReason := string(types.StateChangeReasonClear)
	addReason := string(types.StateChangeReasonAdd)
	removeReason := string(types.StateChangeReasonRemove)
	mintReason := string(types.StateChangeReasonMint)
	burnReason := string(types.StateChangeReasonBurn)
	creditReason := string(types.StateChangeReasonCredit)
	debitReason := string(types.StateChangeReasonDebit)

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 9, "should have exactly 9 state changes")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}

	// 2. FETCH STATE CHANGES IN PARALLEL
	authRequiredQueries := []stateChangeQuery{
		{name: "balanceAuthSetSecondary", account: secondaryAccount, txHash: &txHash, category: &balanceAuthCategory, reason: &setReason},
		{name: "balanceAuthClearSecondary", account: secondaryAccount, txHash: &txHash, category: &balanceAuthCategory, reason: &clearReason},
		{name: "trustlineAdd", account: secondaryAccount, txHash: &txHash, category: &trustlineCategory, reason: &addReason},
		{name: "trustlineRemove", account: secondaryAccount, txHash: &txHash, category: &trustlineCategory, reason: &removeReason},
		{name: "balanceMint", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &mintReason},
		{name: "balanceBurn", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &burnReason},
		{name: "balanceCredit", account: secondaryAccount, txHash: &txHash, category: &balanceCategory, reason: &creditReason},
		{name: "balanceDebit", account: secondaryAccount, txHash: &txHash, category: &balanceCategory, reason: &debitReason},
	}
	authRequiredResults := suite.fetchStateChangesInParallel(ctx, authRequiredQueries, &first)

	// Extract results
	balanceAuthSetSecondary := authRequiredResults["balanceAuthSetSecondary"]
	balanceAuthClearSecondary := authRequiredResults["balanceAuthClearSecondary"]
	trustlineAdd := authRequiredResults["trustlineAdd"]
	trustlineRemove := authRequiredResults["trustlineRemove"]
	balanceMint := authRequiredResults["balanceMint"]
	balanceBurn := authRequiredResults["balanceBurn"]
	balanceCredit := authRequiredResults["balanceCredit"]
	balanceDebit := authRequiredResults["balanceDebit"]

	// Validate results are not nil
	suite.Require().NotNil(balanceAuthSetSecondary, "BALANCE_AUTHORIZATION/SET for secondary should not be nil")
	suite.Require().NotNil(balanceAuthClearSecondary, "BALANCE_AUTHORIZATION/CLEAR for secondary should not be nil")
	suite.Require().NotNil(trustlineAdd, "TRUSTLINE/ADD should not be nil")
	suite.Require().NotNil(trustlineRemove, "TRUSTLINE/REMOVE should not be nil")
	suite.Require().NotNil(balanceMint, "BALANCE/MINT should not be nil")
	suite.Require().NotNil(balanceBurn, "BALANCE/BURN should not be nil")
	suite.Require().NotNil(balanceCredit, "BALANCE/CREDIT should not be nil")
	suite.Require().NotNil(balanceDebit, "BALANCE/DEBIT should not be nil")

	// 3. BALANCE_AUTHORIZATION STATE CHANGES VALIDATION
	// Secondary account should have 2 BALANCE_AUTHORIZATION/SET changes:
	// - One with clawback_enabled flag (from trustline creation inheriting issuer's clawback flag)
	// - One with authorized flag (from SetTrustLineFlags operation)
	suite.Require().Len(balanceAuthSetSecondary.Edges, 2, "should have exactly 2 BALANCE_AUTHORIZATION/SET for secondary")

	// First SET change: clawback_enabled flag from trustline creation
	authSetSecondaryClawback := balanceAuthSetSecondary.Edges[0].Node.(*types.BalanceAuthorizationChange)
	validateStateChangeBase(suite, authSetSecondaryClawback, ledgerNumber)
	validateBalanceAuthorizationChange(suite, authSetSecondaryClawback, secondaryAccount, types.StateChangeReasonSet, []string{"clawback_enabled"})

	// Second SET change: authorized flag from SetTrustLineFlags
	authSetSecondaryAuthorized := balanceAuthSetSecondary.Edges[1].Node.(*types.BalanceAuthorizationChange)
	validateStateChangeBase(suite, authSetSecondaryAuthorized, ledgerNumber)
	validateBalanceAuthorizationChange(suite, authSetSecondaryAuthorized, secondaryAccount, types.StateChangeReasonSet, []string{"authorized"})

	// Secondary account: BALANCE_AUTHORIZATION/CLEAR with "authorized" flag
	suite.Require().Len(balanceAuthClearSecondary.Edges, 1, "should have exactly 1 BALANCE_AUTHORIZATION/CLEAR for secondary")
	authClearSecondary := balanceAuthClearSecondary.Edges[0].Node.(*types.BalanceAuthorizationChange)
	validateStateChangeBase(suite, authClearSecondary, ledgerNumber)
	validateBalanceAuthorizationChange(suite, authClearSecondary, secondaryAccount, types.StateChangeReasonClear, []string{"authorized"})

	// 5. TRUSTLINE STATE CHANGES VALIDATION FOR SECONDARY ACCOUNT
	suite.Require().Len(trustlineAdd.Edges, 1, "should have exactly 1 TRUSTLINE/ADD")
	suite.Require().Len(trustlineRemove.Edges, 1, "should have exactly 1 TRUSTLINE/REMOVE")

	trustlineAddChange := trustlineAdd.Edges[0].Node.(*types.TrustlineChange)
	validateStateChangeBase(suite, trustlineAddChange, ledgerNumber)
	validateTrustlineChange(suite, trustlineAddChange, secondaryAccount, test1ContractAddress, types.StateChangeReasonAdd)

	trustlineRemoveChange := trustlineRemove.Edges[0].Node.(*types.TrustlineChange)
	validateStateChangeBase(suite, trustlineRemoveChange, ledgerNumber)
	validateTrustlineChange(suite, trustlineRemoveChange, secondaryAccount, test1ContractAddress, types.StateChangeReasonRemove)

	// 6. BALANCE STATE CHANGES VALIDATION
	// Validate counts
	suite.Require().Len(balanceMint.Edges, 1, "should have exactly 1 BALANCE/MINT")
	suite.Require().Len(balanceCredit.Edges, 1, "should have exactly 1 BALANCE/CREDIT")
	suite.Require().Len(balanceBurn.Edges, 1, "should have exactly 1 BALANCE/BURN")
	suite.Require().Len(balanceDebit.Edges, 1, "should have exactly 1 BALANCE/DEBIT")

	// Validate MINT
	mintChange := balanceMint.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, mintChange, ledgerNumber)
	validateBalanceChange(suite, mintChange, test1ContractAddress, "10000000000", primaryAccount, types.StateChangeReasonMint)

	// Validate CREDIT
	creditChange := balanceCredit.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, creditChange, ledgerNumber)
	validateBalanceChange(suite, creditChange, test1ContractAddress, "10000000000", secondaryAccount, types.StateChangeReasonCredit)

	// Validate BURN
	burnChange := balanceBurn.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, burnChange, ledgerNumber)
	validateBalanceChange(suite, burnChange, test1ContractAddress, "10000000000", primaryAccount, types.StateChangeReasonBurn)

	// Validate DEBIT (from clawback)
	debitChange := balanceDebit.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, debitChange, ledgerNumber)
	validateBalanceChange(suite, debitChange, test1ContractAddress, "10000000000", secondaryAccount, types.StateChangeReasonDebit)

	// 7. CONSERVATION LAW VALIDATIONS
	totalMint := sumAmounts(suite, balanceMint, test1ContractAddress)
	totalBurn := sumAmounts(suite, balanceBurn, test1ContractAddress)
	suite.Require().Equal(totalMint, totalBurn, "Primary account: MINT should equal BURN for TEST1")
	suite.Require().Equal(int64(10000000000), totalMint, "MINT should be 10000000000")

	totalCredit := sumAmounts(suite, balanceCredit, test1ContractAddress)
	totalDebit := sumAmounts(suite, balanceDebit, test1ContractAddress)
	suite.Require().Equal(totalCredit, totalDebit, "Secondary account: CREDIT should equal DEBIT for TEST1")
	suite.Require().Equal(int64(10000000000), totalCredit, "CREDIT should be 10000000000")
}

func (suite *DataValidationTestSuite) TestAccountMergeOpDataValidation() {
	ctx := context.Background()

	// Find the account merge use case
	useCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/accountMergeOp")
	suite.Require().NotNil(useCase, "accountMergeOp use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validateAccountMergeOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validateAccountMergeStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateAccountMergeOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation, ledgerNumber, types.OperationTypeAccountMerge)
	suite.Require().Equal(types.OperationTypeAccountMerge, operation.OperationType, "operation type should be ACCOUNT_MERGE")
}

func (suite *DataValidationTestSuite) validateAccountMergeStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	accountCategory := "ACCOUNT"
	balanceCategory := "BALANCE"
	reservesCategory := "RESERVES"
	mergeReason := string(types.StateChangeReasonMerge)
	creditReason := string(types.StateChangeReasonCredit)
	debitReason := string(types.StateChangeReasonDebit)
	unsponsorReason := string(types.StateChangeReasonUnsponsor)
	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	sponsoredNewAccount := suite.testEnv.SponsoredNewAccountKP.Address()

	// Verify total count of state changes for this transaction
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 5, "should have exactly 5 state changes")

	for _, edge := range stateChanges.Edges {
		// if edge.Node.GetType() == "RESERVES" {
		// 	keyValue := edge.Node.(*types.ReservesChange).KeyValue
		// 	sponsorAddress := ""
		// 	sponsoredAddress := ""
		// 	if edge.Node.(*types.ReservesChange).SponsorAddress != nil {
		// 		sponsorAddress = *edge.Node.(*types.ReservesChange).SponsorAddress
		// 	}
		// 	if edge.Node.(*types.ReservesChange).SponsoredAddress != nil {
		// 		sponsoredAddress = *edge.Node.(*types.ReservesChange).SponsoredAddress
		// 	}
		// 	str := fmt.Sprintf("%+v\n keyValue: %s\n sponsorAddress: %s\n sponsoredAddress: %s\n", edge.Node, *keyValue, sponsorAddress, sponsoredAddress)
		// 	fmt.Println(str)
		// } else {
		// 	str := fmt.Sprintf("%+v\n", edge.Node)
		// 	fmt.Println(str)
		// }
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	// fmt.Println("primaryAccount", primaryAccount)
	// fmt.Println("sponsoredNewAccount", sponsoredNewAccount)
	// fmt.Println("xlmContractAddress", xlmContractAddress)

	// Fetch state changes in parallel
	accountMergeQueries := []stateChangeQuery{
		{name: "accountMerge", account: primaryAccount, txHash: &txHash, category: &accountCategory, reason: &mergeReason},
		{name: "balanceCredit", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &creditReason},
		{name: "balanceDebit", account: sponsoredNewAccount, txHash: &txHash, category: &balanceCategory, reason: &debitReason},
		{name: "sponsoredReservesUnsponsor", account: sponsoredNewAccount, txHash: &txHash, category: &reservesCategory, reason: &unsponsorReason},
		{name: "sponsorReservesUnsponsor", account: primaryAccount, txHash: &txHash, category: &reservesCategory, reason: &unsponsorReason},
	}
	accountMergeResults := suite.fetchStateChangesInParallel(ctx, accountMergeQueries, &first)

	// Extract results
	accountMergeChanges := accountMergeResults["accountMerge"]
	balanceCreditChanges := accountMergeResults["balanceCredit"]
	balanceDebitChanges := accountMergeResults["balanceDebit"]
	sponsoredReservesUnsponsorChanges := accountMergeResults["sponsoredReservesUnsponsor"]
	sponsorReservesUnsponsorChanges := accountMergeResults["sponsorReservesUnsponsor"]

	// Validate results are not nil
	suite.Require().NotNil(accountMergeChanges, "ACCOUNT/MERGE changes should not be nil")
	suite.Require().NotNil(balanceCreditChanges, "BALANCE/CREDIT changes should not be nil")
	suite.Require().NotNil(balanceDebitChanges, "BALANCE/DEBIT changes should not be nil")
	suite.Require().NotNil(sponsoredReservesUnsponsorChanges, "RESERVES/UNSPONSOR for sponsored account should not be nil")
	suite.Require().NotNil(sponsorReservesUnsponsorChanges, "RESERVES/UNSPONSOR for sponsor account should not be nil")

	// Validate ACCOUNT/MERGE change
	suite.Require().Len(accountMergeChanges.Edges, 1, "should have exactly 1 ACCOUNT/MERGE change")
	accountChange := accountMergeChanges.Edges[0].Node.(*types.AccountChange)
	validateStateChangeBase(suite, accountChange, ledgerNumber)
	suite.Require().Equal(types.StateChangeCategoryAccount, accountChange.GetType(), "should be ACCOUNT type")
	suite.Require().Equal(types.StateChangeReasonMerge, accountChange.GetReason(), "reason should be MERGE")
	suite.Require().Equal(primaryAccount, accountChange.GetAccountID(), "account ID should be the destination account (receiving the merge)")

	// Validate BALANCE/CREDIT change
	suite.Require().Len(balanceCreditChanges.Edges, 1, "should have exactly 1 BALANCE/CREDIT change")
	balanceCreditChange := balanceCreditChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, balanceCreditChange, ledgerNumber)
	validateBalanceChange(suite, balanceCreditChange, xlmContractAddress, "50000000", primaryAccount, types.StateChangeReasonCredit)

	// 5. RESERVES/UNSPONSOR STATE CHANGES VALIDATION FOR SPONSORED ACCOUNT
	suite.Require().Len(sponsoredReservesUnsponsorChanges.Edges, 1, "should have exactly 1 RESERVES/UNSPONSOR for sponsored account")
	sponsoredReservesChange := sponsoredReservesUnsponsorChanges.Edges[0].Node.(*types.ReservesChange)
	validateStateChangeBase(suite, sponsoredReservesChange, ledgerNumber)
	validateReservesSponsorshipChangeForSponsoredAccount(suite, sponsoredReservesChange, sponsoredNewAccount, types.StateChangeReasonUnsponsor, primaryAccount, "", "")
	
	// Validate BALANCE/DEBIT change
	suite.Require().Len(balanceDebitChanges.Edges, 1, "should have exactly 1 BALANCE/DEBIT change")
	balanceDebitChange := balanceDebitChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, balanceDebitChange, ledgerNumber)
	validateBalanceChange(suite, balanceDebitChange, xlmContractAddress, "50000000", sponsoredNewAccount, types.StateChangeReasonDebit)

	// Validate RESERVES/UNSPONSOR for sponsored account
	suite.Require().Len(sponsoredReservesUnsponsorChanges.Edges, 1, "should have exactly 1 RESERVES/UNSPONSOR for sponsored account")
	sponsoredReservesChange = sponsoredReservesUnsponsorChanges.Edges[0].Node.(*types.ReservesChange)
	validateStateChangeBase(suite, sponsoredReservesChange, ledgerNumber)
	validateReservesSponsorshipChangeForSponsoredAccount(suite, sponsoredReservesChange, sponsoredNewAccount, types.StateChangeReasonUnsponsor, primaryAccount, "", "")

	// Validate RESERVES/UNSPONSOR for sponsor account
	suite.Require().Len(sponsorReservesUnsponsorChanges.Edges, 1, "should have exactly 1 RESERVES/UNSPONSOR for sponsor account")
	sponsorReservesChange := sponsorReservesUnsponsorChanges.Edges[0].Node.(*types.ReservesChange)
	validateStateChangeBase(suite, sponsorReservesChange, ledgerNumber)
	validateReservesSponsorshipChangeForSponsoringAccount(suite, sponsorReservesChange, primaryAccount, types.StateChangeReasonUnsponsor, sponsoredNewAccount, "", "")
}

func (suite *DataValidationTestSuite) TestInvokeContractOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating invoke-contract operations data...")

	// Find the auth-required use case
	useCases := []string{
		"Soroban/invokeContractOp/SorobanAuth",
		"Soroban/invokeContractOp/SourceAccountAuth",
	}
	for _, useCaseName := range useCases {
		useCase := infrastructure.FindUseCase(suite.testEnv.UseCases, useCaseName)
		suite.Require().NotNil(useCase, fmt.Sprintf("%s use case not found", useCaseName))
		suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

		txHash := useCase.GetTransactionResult.Hash
		tx := validateTransactionBase(suite, ctx, txHash)
		suite.validateInvokeContractOperations(ctx, txHash, int64(tx.LedgerNumber))
		suite.validateInvokeContractStateChanges(ctx, txHash, int64(tx.LedgerNumber))
	}
}

func (suite *DataValidationTestSuite) validateInvokeContractOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeInvokeHostFunction,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateInvokeContractStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(15)

	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	balanceCategory := "BALANCE"
	creditReason := string(types.StateChangeReasonCredit)
	debitReason := string(types.StateChangeReasonDebit)

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 11 state changes")

	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}

	// Fetch state changes in parallel
	balanceQueries := []stateChangeQuery{
		{name: "balanceCredit", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &creditReason},
		{name: "balanceDebit", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &debitReason},
	}
	balanceResults := suite.fetchStateChangesInParallel(ctx, balanceQueries, &first)

	balanceCreditChanges := balanceResults["balanceCredit"]
	balanceDebitChanges := balanceResults["balanceDebit"]

	// Validate results are not nil
	suite.Require().NotNil(balanceCreditChanges, "BALANCE/CREDIT changes should not be nil")
	suite.Require().NotNil(balanceDebitChanges, "BALANCE/DEBIT changes should not be nil")

	// Validate BALANCE/CREDIT change
	suite.Require().Len(balanceCreditChanges.Edges, 1, "should have exactly 1 BALANCE/CREDIT change")
	balanceCreditChange := balanceCreditChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, balanceCreditChange, ledgerNumber)
	validateBalanceChange(suite, balanceCreditChange, xlmContractAddress, "100000000", primaryAccount, types.StateChangeReasonCredit)

	// Validate BALANCE/DEBIT change
	suite.Require().Len(balanceDebitChanges.Edges, 1, "should have exactly 1 BALANCE/DEBIT change")
	balanceDebitChange := balanceDebitChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, balanceDebitChange, ledgerNumber)
	validateBalanceChange(suite, balanceDebitChange, xlmContractAddress, "100000000", primaryAccount, types.StateChangeReasonDebit)
}

func (suite *DataValidationTestSuite) TestCreateClaimableBalanceOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating create claimable balance operations data...")

	// Find the claimable balance use case
	useCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/createClaimableBalanceOps")
	suite.Require().NotNil(useCase, "createClaimableBalanceOps use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validateCreateClaimableBalanceOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validateCreateClaimableBalanceStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateCreateClaimableBalanceOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 4, "should have exactly 4 operations")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeChangeTrust,            // Create trustline
		types.OperationTypeSetTrustLineFlags,      // Authorize trustline
		types.OperationTypeCreateClaimableBalance, // Create claimable balance
		types.OperationTypeCreateClaimableBalance, // Create claimable balance
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateCreateClaimableBalanceStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(20)

	// Setup: Compute expected values from fixtures
	test3Asset := xdr.MustNewCreditAsset("TEST3", suite.testEnv.PrimaryAccountKP.Address())
	test3ContractAddress := suite.getAssetContractAddress(test3Asset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	secondaryAccount := suite.testEnv.SecondaryAccountKP.Address()

	// Define filter constants
	trustlineCategory := "TRUSTLINE"
	balanceAuthCategory := "BALANCE_AUTHORIZATION"
	balanceCategory := "BALANCE"
	reservesCategory := "RESERVES"
	sponsorReason := string(types.StateChangeReasonSponsor)
	setReason := string(types.StateChangeReasonSet)
	addReason := string(types.StateChangeReasonAdd)
	mintReason := string(types.StateChangeReasonMint)

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}

	// 2. FETCH STATE CHANGES IN PARALLEL
	claimableBalanceQueries := []stateChangeQuery{
		{name: "trustlineAdd", account: secondaryAccount, txHash: &txHash, category: &trustlineCategory, reason: &addReason},
		{name: "balanceAuthSet", account: secondaryAccount, txHash: &txHash, category: &balanceAuthCategory, reason: &setReason},
		{name: "balanceMint", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &mintReason},
		{name: "reservesSponsorForSponsor", account: primaryAccount, txHash: &txHash, category: &reservesCategory, reason: &sponsorReason},
	}
	claimableBalanceResults := suite.fetchStateChangesInParallel(ctx, claimableBalanceQueries, &first)

	// Extract results
	trustlineAdd := claimableBalanceResults["trustlineAdd"]
	balanceAuthSet := claimableBalanceResults["balanceAuthSet"]
	balanceMint := claimableBalanceResults["balanceMint"]
	reservesSponsorForSponsor := claimableBalanceResults["reservesSponsorForSponsor"]

	// Validate results are not nil
	suite.Require().NotNil(trustlineAdd, "TRUSTLINE/ADD should not be nil")
	suite.Require().NotNil(balanceAuthSet, "BALANCE_AUTHORIZATION/SET should not be nil")
	suite.Require().NotNil(balanceMint, "BALANCE/MINT should not be nil")
	suite.Require().NotNil(reservesSponsorForSponsor, "RESERVES/SPONSOR for sponsor should not be nil")

	// 3. TRUSTLINE STATE CHANGES VALIDATION FOR SECONDARY ACCOUNT
	suite.Require().Len(trustlineAdd.Edges, 1, "should have exactly 1 TRUSTLINE/ADD")
	trustlineAddChange := trustlineAdd.Edges[0].Node.(*types.TrustlineChange)
	validateStateChangeBase(suite, trustlineAddChange, ledgerNumber)
	validateTrustlineChange(suite, trustlineAddChange, secondaryAccount, test3ContractAddress, types.StateChangeReasonAdd)

	// 4. BALANCE_AUTHORIZATION STATE CHANGES VALIDATION
	// Secondary account should have 2 BALANCE_AUTHORIZATION/SET changes:
	// - One with clawback_enabled flag (from trustline creation inheriting issuer's clawback flag)
	// - One with authorized flag (from SetTrustLineFlags operation)
	suite.Require().Len(balanceAuthSet.Edges, 2, "should have exactly 2 BALANCE_AUTHORIZATION/SET for secondary")
	authSetSecondary := balanceAuthSet.Edges[0].Node.(*types.BalanceAuthorizationChange)
	validateStateChangeBase(suite, authSetSecondary, ledgerNumber)
	validateBalanceAuthorizationChange(suite, authSetSecondary, secondaryAccount, types.StateChangeReasonSet, []string{"clawback_enabled"})

	// Second SET change: authorized flag from SetTrustLineFlags
	authSetSecondaryAuthorized := balanceAuthSet.Edges[1].Node.(*types.BalanceAuthorizationChange)
	validateStateChangeBase(suite, authSetSecondaryAuthorized, ledgerNumber)
	validateBalanceAuthorizationChange(suite, authSetSecondaryAuthorized, secondaryAccount, types.StateChangeReasonSet, []string{"authorized"})

	// 5. BALANCE STATE CHANGES VALIDATION - 2 claimable balances are created
	suite.Require().Len(balanceMint.Edges, 2, "should have exactly 2 BALANCE/MINT")
	for _, edge := range balanceMint.Edges {
		mintChange := edge.Node.(*types.StandardBalanceChange)
		validateStateChangeBase(suite, mintChange, ledgerNumber)
		validateBalanceChange(suite, mintChange, test3ContractAddress, "10000000", primaryAccount, types.StateChangeReasonMint)
	}

	// 6. 2 RESERVES/SPONSOR STATE CHANGES VALIDATION FOR SPONSORING ACCOUNT for 2 claimable balances
	suite.Require().Len(reservesSponsorForSponsor.Edges, 2, "should have exactly 2 RESERVES/SPONSOR for sponsor")
	change := reservesSponsorForSponsor.Edges[0].Node.(*types.ReservesChange)
	validateStateChangeBase(suite, change, ledgerNumber)
	validateReservesSponsorshipChangeForSponsoringAccount(suite, change, primaryAccount, types.StateChangeReasonSponsor, "", "balance_id", suite.testEnv.ClaimBalanceID)

	change = reservesSponsorForSponsor.Edges[1].Node.(*types.ReservesChange)
	validateStateChangeBase(suite, change, ledgerNumber)
	validateReservesSponsorshipChangeForSponsoringAccount(suite, change, primaryAccount, types.StateChangeReasonSponsor, "", "balance_id", suite.testEnv.ClawbackBalanceID)
}

func (suite *DataValidationTestSuite) TestClaimClaimableBalanceDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating claim claimable balance operation data...")

	// Find the claim claimable balance use case
	useCase := infrastructure.FindUseCase(suite.testEnv.ClaimAndClawbackUseCases, "Stellarclassic/claimClaimableBalanceOp")
	suite.Require().NotNil(useCase, "claimClaimableBalanceOp use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validateClaimClaimableBalanceOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validateClaimClaimableBalanceStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateClaimClaimableBalanceOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation, ledgerNumber, types.OperationTypeClaimClaimableBalance)
}

func (suite *DataValidationTestSuite) validateClaimClaimableBalanceStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)

	// Setup: Compute expected values from fixtures
	test3Asset := xdr.MustNewCreditAsset("TEST3", suite.testEnv.PrimaryAccountKP.Address())
	test3ContractAddress := suite.getAssetContractAddress(test3Asset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	secondaryAccount := suite.testEnv.SecondaryAccountKP.Address()

	// Define filter constants
	balanceCategory := "BALANCE"
	creditReason := string(types.StateChangeReasonCredit)
	reservesCategory := "RESERVES"
	unsponsorReason := string(types.StateChangeReasonUnsponsor)

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 2 state changes")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}

	// 2. FETCH BALANCE/CREDIT STATE CHANGE
	claimQueries := []stateChangeQuery{
		{name: "balanceCredit", account: secondaryAccount, txHash: &txHash, category: &balanceCategory, reason: &creditReason},
		{name: "reservesUnsponsorForSponsor", account: primaryAccount, txHash: &txHash, category: &reservesCategory, reason: &unsponsorReason},
	}
	claimResults := suite.fetchStateChangesInParallel(ctx, claimQueries, &first)

	// Extract and validate results
	balanceCreditChanges := claimResults["balanceCredit"]
	reservesUnsponsorForSponsor := claimResults["reservesUnsponsorForSponsor"]
	suite.Require().NotNil(balanceCreditChanges, "BALANCE/CREDIT changes should not be nil")
	suite.Require().NotNil(reservesUnsponsorForSponsor, "RESERVES/UNSPONSOR for sponsor should not be nil")

	// 3. VALIDATE BALANCE/CREDIT CHANGE
	suite.Require().Len(balanceCreditChanges.Edges, 1, "should have exactly 1 BALANCE/CREDIT change")
	balanceCreditChange := balanceCreditChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, balanceCreditChange, ledgerNumber)
	validateBalanceChange(suite, balanceCreditChange, test3ContractAddress, "10000000", secondaryAccount, types.StateChangeReasonCredit)

	// 4. RESERVES/UNSPONSOR STATE CHANGES VALIDATION FOR SPONSORING ACCOUNT
	suite.Require().Len(reservesUnsponsorForSponsor.Edges, 1, "should have exactly 1 RESERVES/UNSPONSOR for sponsor")
	reservesUnsponsorForSponsorChange := reservesUnsponsorForSponsor.Edges[0].Node.(*types.ReservesChange)
	validateStateChangeBase(suite, reservesUnsponsorForSponsorChange, ledgerNumber)
	validateReservesSponsorshipChangeForSponsoringAccount(suite, reservesUnsponsorForSponsorChange, primaryAccount, types.StateChangeReasonUnsponsor, "", "balance_id", suite.testEnv.ClaimBalanceID)
}

func (suite *DataValidationTestSuite) TestClawbackClaimableBalanceDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating clawback claimable balance operation data...")

	// Find the clawback claimable balance use case
	useCase := infrastructure.FindUseCase(suite.testEnv.ClaimAndClawbackUseCases, "Stellarclassic/clawbackClaimableBalanceOp")
	suite.Require().NotNil(useCase, "clawbackClaimableBalanceOp use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validateClawbackClaimableBalanceOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validateClawbackClaimableBalanceStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateClawbackClaimableBalanceOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation, ledgerNumber, types.OperationTypeClawbackClaimableBalance)
	suite.Require().Equal(types.OperationTypeClawbackClaimableBalance, operation.OperationType, "operation type should be CLAWBACK_CLAIMABLE_BALANCE")
}

func (suite *DataValidationTestSuite) validateClawbackClaimableBalanceStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)

	// Setup: Compute expected values from fixtures
	test3Asset := xdr.MustNewCreditAsset("TEST3", suite.testEnv.PrimaryAccountKP.Address())
	test3ContractAddress := suite.getAssetContractAddress(test3Asset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()

	// Define filter constants
	balanceCategory := "BALANCE"
	burnReason := string(types.StateChangeReasonBurn)
	reservesCategory := "RESERVES"
	unsponsorReason := string(types.StateChangeReasonUnsponsor)

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 2 state change")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}

	// 2. FETCH BALANCE/BURN STATE CHANGE
	clawbackQueries := []stateChangeQuery{
		{name: "balanceBurn", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &burnReason},
		{name: "reservesUnsponsorForSponsor", account: primaryAccount, txHash: &txHash, category: &reservesCategory, reason: &unsponsorReason},
	}
	clawbackResults := suite.fetchStateChangesInParallel(ctx, clawbackQueries, &first)

	// Extract and validate results
	balanceBurnChanges := clawbackResults["balanceBurn"]
	reservesUnsponsorForSponsor := clawbackResults["reservesUnsponsorForSponsor"]
	suite.Require().NotNil(balanceBurnChanges, "BALANCE/BURN changes should not be nil")
	suite.Require().NotNil(reservesUnsponsorForSponsor, "RESERVES/UNSPONSOR for sponsor should not be nil")

	// 3. VALIDATE BALANCE/BURN CHANGE
	suite.Require().Len(balanceBurnChanges.Edges, 1, "should have exactly 1 BALANCE/BURN change")
	balanceBurnChange := balanceBurnChanges.Edges[0].Node.(*types.StandardBalanceChange)
	validateStateChangeBase(suite, balanceBurnChange, ledgerNumber)
	validateBalanceChange(suite, balanceBurnChange, test3ContractAddress, "10000000", primaryAccount, types.StateChangeReasonBurn)

	// 4. RESERVES/UNSPONSOR STATE CHANGES VALIDATION FOR SPONSORING ACCOUNT FOR CLAWBACK BALANCE
	suite.Require().Len(reservesUnsponsorForSponsor.Edges, 1, "should have exactly 1 RESERVES/UNSPONSOR for sponsor")
	reservesUnsponsorForSponsorChange := reservesUnsponsorForSponsor.Edges[0].Node.(*types.ReservesChange)
	validateStateChangeBase(suite, reservesUnsponsorForSponsorChange, ledgerNumber)
	validateReservesSponsorshipChangeForSponsoringAccount(suite, reservesUnsponsorForSponsorChange, primaryAccount, types.StateChangeReasonUnsponsor, "", "balance_id", suite.testEnv.ClawbackBalanceID)
}

func (suite *DataValidationTestSuite) TestClearAuthFlagsOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating clear auth flags operations data...")

	// Find the clear auth flags use case
	useCase := infrastructure.FindUseCase(suite.testEnv.ClaimAndClawbackUseCases, "Stellarclassic/clearAuthFlagsOps")
	suite.Require().NotNil(useCase, "clearAuthFlagsOps use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validateClearAuthFlagsOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validateClearAuthFlagsStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateClearAuthFlagsOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeSetOptions,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateClearAuthFlagsStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)

	// Setup: Get primary account
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()

	// Define filter constants
	flagsCategory := "FLAGS"
	clearReason := string(types.StateChangeReasonClear)

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 1, "should have exactly 1 state change")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}

	// 2. FETCH FLAGS/CLEAR STATE CHANGE
	clearAuthFlagsQueries := []stateChangeQuery{
		{name: "flagsClearPrimary", account: primaryAccount, txHash: &txHash, category: &flagsCategory, reason: &clearReason},
	}
	clearAuthFlagsResults := suite.fetchStateChangesInParallel(ctx, clearAuthFlagsQueries, &first)

	// Extract results
	flagsClearPrimary := clearAuthFlagsResults["flagsClearPrimary"]
	suite.Require().NotNil(flagsClearPrimary, "FLAGS/CLEAR for primary should not be nil")

	// 3. FLAGS STATE CHANGES VALIDATION FOR PRIMARY ACCOUNT
	suite.Require().Len(flagsClearPrimary.Edges, 1, "should have exactly 1 FLAGS/CLEAR change for primary")
	expectedFlags := []string{"auth_required", "auth_revocable", "auth_clawback_enabled"}
	flagsClearChange := flagsClearPrimary.Edges[0].Node.(*types.FlagsChange)
	validateStateChangeBase(suite, flagsClearChange, ledgerNumber)
	validateFlagsChange(suite, flagsClearChange, primaryAccount, types.StateChangeReasonClear, expectedFlags)
}

// func (suite *DataValidationTestSuite) TestLiquidityPoolOpsDataValidation() {
// 	ctx := context.Background()
// 	log.Ctx(ctx).Info("🔍 Validating liquidity pool operations data...")

// 	// Find the liquidity pool use case
// 	useCase := infrastructure.FindUseCase(suite.testEnv.UseCases,"Stellarclassic/liquidityPoolOps")
// 	suite.Require().NotNil(useCase, "liquidityPoolOps use case not found")
// 	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

// 	txHash := useCase.GetTransactionResult.Hash
// 	tx := validateTransactionBase(suite, ctx, txHash)
// 	suite.validateLiquidityPoolOperations(ctx, txHash, int64(tx.LedgerNumber))
// 	suite.validateLiquidityPoolStateChanges(ctx, txHash, int64(tx.LedgerNumber))
// }

// func (suite *DataValidationTestSuite) validateLiquidityPoolOperations(ctx context.Context, txHash string, ledgerNumber int64) {
// 	first := int32(10)
// 	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
// 	suite.Require().NoError(err, "failed to get transaction operations")
// 	suite.Require().NotNil(operations, "operations should not be nil")
// 	suite.Require().Len(operations.Edges, 4, "should have exactly 4 operations")

// 	expectedOpTypes := []types.OperationType{
// 		types.OperationTypeChangeTrust,           // Create trustline to pool
// 		types.OperationTypeLiquidityPoolDeposit,  // Deposit into pool
// 		types.OperationTypeLiquidityPoolWithdraw, // Withdraw from pool
// 		types.OperationTypeChangeTrust,           // Remove trustline to pool
// 	}

// 	for i, edge := range operations.Edges {
// 		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
// 		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
// 	}
// }

// func (suite *DataValidationTestSuite) validateLiquidityPoolStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
// 	first := int32(30)

// 	// Setup: Compute expected values from fixtures
// 	test2Asset := xdr.MustNewCreditAsset("TEST2", suite.testEnv.PrimaryAccountKP.Address())
// 	test2ContractAddress := suite.getAssetContractAddress(test2Asset)
// 	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
// 	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()

// 	// 1. TOTAL STATE CHANGE COUNT VALIDATION
// 	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
// 	suite.Require().NoError(err, "failed to get transaction state changes")
// 	suite.Require().NotNil(stateChanges, "state changes should not be nil")

// 	// Validate base fields for all state changes
// 	for _, edge := range stateChanges.Edges {
// 		validateStateChangeBase(suite, edge.Node, ledgerNumber)
// 	}

// 	// 2. VALIDATE PRESENCE OF KEY STATE CHANGE CATEGORIES
// 	balanceCategory := "BALANCE"
// 	trustlineCategory := "TRUSTLINE"
// 	addReason := string(types.StateChangeReasonAdd)
// 	removeReason := string(types.StateChangeReasonRemove)

// 	// Fetch state changes for validation
// 	lpQueries := []stateChangeQuery{
// 		{name: "trustlineAdd", account: primaryAccount, txHash: &txHash, category: &trustlineCategory, reason: &addReason},
// 		{name: "trustlineRemove", account: primaryAccount, txHash: &txHash, category: &trustlineCategory, reason: &removeReason},
// 		{name: "balanceChanges", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: nil},
// 	}
// 	lpResults := suite.fetchStateChangesInParallel(ctx, lpQueries, &first)

// 	// Extract results
// 	trustlineAdd := lpResults["trustlineAdd"]
// 	trustlineRemove := lpResults["trustlineRemove"]
// 	balanceChanges := lpResults["balanceChanges"]

// 	// Validate results are not nil
// 	suite.Require().NotNil(trustlineAdd, "TRUSTLINE/ADD should not be nil")
// 	suite.Require().NotNil(trustlineRemove, "TRUSTLINE/REMOVE should not be nil")
// 	suite.Require().NotNil(balanceChanges, "BALANCE changes should not be nil")

// 	// 3. TRUSTLINE VALIDATION
// 	suite.Require().Len(trustlineAdd.Edges, 1, "should have exactly 1 TRUSTLINE/ADD for liquidity pool")
// 	suite.Require().Len(trustlineRemove.Edges, 1, "should have exactly 1 TRUSTLINE/REMOVE for liquidity pool")

// 	// 4. BALANCE CHANGES VALIDATION
// 	// Should have multiple balance changes for deposits and withdrawals of both assets
// 	suite.Require().NotEmpty(balanceChanges.Edges, "should have balance changes")

// 	// Verify we have changes for both TEST2 and XLM
// 	foundTest2 := false
// 	foundXLM := false
// 	for _, edge := range balanceChanges.Edges {
// 		bc := edge.Node.(*types.StandardBalanceChange)
// 		if bc.TokenID == test2ContractAddress {
// 			foundTest2 = true
// 		}
// 		if bc.TokenID == xlmContractAddress {
// 			foundXLM = true
// 		}
// 	}
// 	suite.Require().True(foundTest2, "should have TEST2 balance changes")
// 	suite.Require().True(foundXLM, "should have XLM balance changes")
// }

// func (suite *DataValidationTestSuite) TestRevokeSponsorshipOpsDataValidation() {
// 	ctx := context.Background()
// 	log.Ctx(ctx).Info("🔍 Validating revoke sponsorship operations data...")

// 	// Find the revoke sponsorship use case
// 	useCase := infrastructure.FindUseCase(suite.testEnv.UseCases,"Stellarclassic/revokeSponsorshipOps")
// 	suite.Require().NotNil(useCase, "revokeSponsorshipOps use case not found")
// 	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

// 	txHash := useCase.GetTransactionResult.Hash
// 	tx := validateTransactionBase(suite, ctx, txHash)
// 	suite.validateRevokeSponsorshipOperations(ctx, txHash, int64(tx.LedgerNumber))
// 	suite.validateRevokeSponsorshipStateChanges(ctx, txHash, int64(tx.LedgerNumber))
// }

// func (suite *DataValidationTestSuite) validateRevokeSponsorshipOperations(ctx context.Context, txHash string, ledgerNumber int64) {
// 	first := int32(10)
// 	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &first, nil, nil, nil)
// 	suite.Require().NoError(err, "failed to get transaction operations")
// 	suite.Require().NotNil(operations, "operations should not be nil")
// 	suite.Require().Len(operations.Edges, 5, "should have exactly 5 operations")

// 	expectedOpTypes := []types.OperationType{
// 		types.OperationTypeBeginSponsoringFutureReserves, // Begin sponsorship
// 		types.OperationTypeManageData,                    // Create sponsored data entry
// 		types.OperationTypeEndSponsoringFutureReserves,   // End sponsorship
// 		types.OperationTypeRevokeSponsorship,             // Revoke sponsorship
// 		types.OperationTypeManageData,                    // Remove data entry
// 	}

// 	for i, edge := range operations.Edges {
// 		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
// 		suite.Require().Equal(expectedOpTypes[i], edge.Node.OperationType, "operation type mismatch at index %d", i)
// 	}
// }

// func (suite *DataValidationTestSuite) validateRevokeSponsorshipStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
// 	first := int32(20)

// 	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
// 	secondaryAccount := suite.testEnv.SecondaryAccountKP.Address()

// 	// Define filter constants
// 	metadataCategory := "METADATA"
// 	reservesCategory := "RESERVES"
// 	sponsorReason := string(types.StateChangeReasonSponsor)
// 	unsponsorReason := string(types.StateChangeReasonUnsponsor)
// 	dataEntryReason := string(types.StateChangeReasonDataEntry)

// 	// 1. TOTAL STATE CHANGE COUNT VALIDATION
// 	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &first, nil, nil, nil)
// 	suite.Require().NoError(err, "failed to get transaction state changes")
// 	suite.Require().NotNil(stateChanges, "state changes should not be nil")

// 	// Validate base fields for all state changes
// 	for _, edge := range stateChanges.Edges {
// 		validateStateChangeBase(suite, edge.Node, ledgerNumber)
// 	}

// 	// 2. FETCH STATE CHANGES IN PARALLEL
// 	revokeSponsorshipQueries := []stateChangeQuery{
// 		{name: "metadataDataEntry", account: secondaryAccount, txHash: &txHash, category: &metadataCategory, reason: &dataEntryReason},
// 		{name: "reservesSponsor", account: secondaryAccount, txHash: &txHash, category: &reservesCategory, reason: &sponsorReason},
// 		{name: "reservesUnsponsor", account: secondaryAccount, txHash: &txHash, category: &reservesCategory, reason: &unsponsorReason},
// 		{name: "primaryReservesSponsor", account: primaryAccount, txHash: &txHash, category: &reservesCategory, reason: &sponsorReason},
// 		{name: "primaryReservesUnsponsor", account: primaryAccount, txHash: &txHash, category: &reservesCategory, reason: &unsponsorReason},
// 	}
// 	revokeSponsorshipResults := suite.fetchStateChangesInParallel(ctx, revokeSponsorshipQueries, &first)

// 	// Extract results
// 	metadataDataEntry := revokeSponsorshipResults["metadataDataEntry"]
// 	reservesSponsor := revokeSponsorshipResults["reservesSponsor"]
// 	reservesUnsponsor := revokeSponsorshipResults["reservesUnsponsor"]
// 	primaryReservesSponsor := revokeSponsorshipResults["primaryReservesSponsor"]
// 	primaryReservesUnsponsor := revokeSponsorshipResults["primaryReservesUnsponsor"]

// 	// Validate results are not nil
// 	suite.Require().NotNil(metadataDataEntry, "METADATA/DATA_ENTRY should not be nil")
// 	suite.Require().NotNil(reservesSponsor, "RESERVES/SPONSOR for secondary should not be nil")
// 	suite.Require().NotNil(reservesUnsponsor, "RESERVES/UNSPONSOR for secondary should not be nil")
// 	suite.Require().NotNil(primaryReservesSponsor, "RESERVES/SPONSOR for primary should not be nil")
// 	suite.Require().NotNil(primaryReservesUnsponsor, "RESERVES/UNSPONSOR for primary should not be nil")

// 	// 3. METADATA STATE CHANGES VALIDATION
// 	suite.Require().NotEmpty(metadataDataEntry.Edges, "should have METADATA/DATA_ENTRY changes")

// 	// 4. RESERVES STATE CHANGES VALIDATION
// 	suite.Require().NotEmpty(reservesSponsor.Edges, "should have RESERVES/SPONSOR changes for secondary")
// 	suite.Require().NotEmpty(reservesUnsponsor.Edges, "should have RESERVES/UNSPONSOR changes for secondary")

// 	// Validate sponsorship establishment
// 	for _, edge := range reservesSponsor.Edges {
// 		rc := edge.Node.(*types.ReservesChange)
// 		validateStateChangeBase(suite, rc, ledgerNumber)
// 		suite.Require().Equal(types.StateChangeCategoryReserves, rc.GetType(), "should be RESERVES type")
// 		suite.Require().Equal(types.StateChangeReasonSponsor, rc.GetReason(), "reason should be SPONSOR")
// 	}

// 	// Validate sponsorship revocation
// 	for _, edge := range reservesUnsponsor.Edges {
// 		rc := edge.Node.(*types.ReservesChange)
// 		validateStateChangeBase(suite, rc, ledgerNumber)
// 		suite.Require().Equal(types.StateChangeCategoryReserves, rc.GetType(), "should be RESERVES type")
// 		suite.Require().Equal(types.StateChangeReasonUnsponsor, rc.GetReason(), "reason should be UNSPONSOR")
// 	}
// }
