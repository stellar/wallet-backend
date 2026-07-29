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
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient"
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
	category *types.StateChangeCategory
	reason   *types.StateChangeReason
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
				ctx, query.account,
				&wbclient.StateChangeFilter{
					TransactionHash: query.txHash,
					Category:        query.category,
					Reason:          query.reason,
				},
				nil,
				&wbclient.Page{First: first},
			)
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
	suite.Require().NotZero(tx.FeeCharged, "fee charged should not be zero")
	suite.Require().NotEmpty(tx.ResultCode, "result code should not be empty")
	suite.Require().NotZero(tx.LedgerNumber, "ledger number should not be zero")
	suite.Require().False(tx.LedgerCreatedAt.IsZero(), "ledger created at should not be zero")
	suite.Require().False(tx.IngestedAt.IsZero(), "ingested at should not be zero")

	if tx.IsFeeBump {
		suite.Require().Equal("TransactionResultCodeTxFeeBumpInnerSuccess", tx.ResultCode, "result code does not match")
	} else {
		suite.Require().Equal("TransactionResultCodeTxSuccess", tx.ResultCode, "result code does not match")
	}

	return tx
}

// validateOperationBase validates common operation fields
func validateOperationBase(suite *DataValidationTestSuite, op *types.Operation, expectedLedgerNumber int64, expectedOperationType types.OperationType) {
	suite.Require().NotNil(op, "operation should not be nil")
	suite.Require().Equal(expectedOperationType, op.Type, "operation type mismatch")
	suite.Require().NotEmpty(op.OperationXdr, "operation XDR should not be empty")
	suite.Require().Equal(processors.OpSuccess, op.ResultCode, "operation result code does not match")
	suite.Require().True(op.Successful, "operation is not successful")
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

// validateBalanceChange validates a balance state change (BALANCE category, one of
// DEBIT/CREDIT/MINT/BURN). Transaction-fee rows are BalanceChange too and are not asserted here.
func validateBalanceChange(suite *DataValidationTestSuite, bc *types.BalanceChange, expectedTokenID, expectedAmount string, expectedReason types.StateChangeReason) {
	suite.Require().NotNil(bc, "balance change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryBalance, bc.GetCategory(), "should be BALANCE category")
	suite.Require().Equal(expectedReason, bc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedTokenID, bc.TokenID, "token ID mismatch")
	suite.Require().Equal(expectedAmount, bc.Amount, "amount mismatch")
}

// validateAccountCreatedChange validates an account-creation state change (ACCOUNT/CREATE),
// which carries the creator's address: the funder for a classic account, the deployer for a
// contract.
func validateAccountCreatedChange(suite *DataValidationTestSuite, ac *types.AccountCreatedChange, expectedCreatorAddress string) {
	suite.Require().NotNil(ac, "account created change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryAccount, ac.GetCategory(), "should be ACCOUNT category")
	suite.Require().Equal(types.StateChangeReasonCreate, ac.GetReason(), "reason mismatch")
	suite.Require().NotEmpty(ac.CreatorAddress, "creator address should not be empty")
	suite.Require().Equal(expectedCreatorAddress, ac.CreatorAddress, "creator address mismatch")
}

// validateSignerAddedChange validates a signer-added state change (SIGNER/ADD). The signer weight is now
// a typed field, so it is asserted directly rather than decoded from a JSON blob.
func validateSignerAddedChange(suite *DataValidationTestSuite, sc *types.SignerAddedChange, expectedSignerAddress string, expectedWeight int32) {
	suite.Require().NotNil(sc, "signer added change should not be nil")
	suite.Require().Equal(types.StateChangeCategorySigner, sc.GetCategory(), "should be SIGNER category")
	suite.Require().Equal(types.StateChangeReasonAdd, sc.GetReason(), "reason mismatch")
	suite.Require().NotEmpty(sc.SignerAddress, "signer address should not be empty")
	suite.Require().Equal(expectedSignerAddress, sc.SignerAddress, "signer address mismatch")
	suite.Require().Equal(expectedWeight, sc.NewWeight, "signer weight mismatch")
}

// validateDataEntryAddedChange validates a data-entry creation (DATA_ENTRY/ADD), which carries
// only the entry's new value.
func validateDataEntryAddedChange(suite *DataValidationTestSuite, dc *types.DataEntryAddedChange, expectedName, expectedValue string) {
	suite.Require().NotNil(dc, "data entry added change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryDataEntry, dc.GetCategory(), "should be DATA_ENTRY category")
	suite.Require().Equal(types.StateChangeReasonAdd, dc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedName, dc.Name, "data entry name mismatch")
	assertDecodedDataEntryValue(suite, "value", dc.Value, expectedValue)
}

// validateDataEntryRemovedChange validates a data-entry removal (DATA_ENTRY/REMOVE), which
// carries only the value the entry held when removed.
func validateDataEntryRemovedChange(suite *DataValidationTestSuite, dc *types.DataEntryRemovedChange, expectedName, expectedOldValue string) {
	suite.Require().NotNil(dc, "data entry removed change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryDataEntry, dc.GetCategory(), "should be DATA_ENTRY category")
	suite.Require().Equal(types.StateChangeReasonRemove, dc.GetReason(), "reason mismatch")
	suite.Require().Equal(expectedName, dc.Name, "data entry name mismatch")
	assertDecodedDataEntryValue(suite, "oldValue", dc.OldValue, expectedOldValue)
}

// assertDecodedDataEntryValue decodes a base64-encoded data-entry value and compares it to the
// expected raw bytes.
func assertDecodedDataEntryValue(suite *DataValidationTestSuite, fieldName, encoded, expectedValue string) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	suite.Require().NoError(err, "failed to decode %s: %s", fieldName, encoded)
	suite.Require().Equal(expectedValue, string(decoded), "%s does not match", fieldName)
}

// sumAmounts aggregates amounts from balance changes for a specific token and returns the total as int64
func sumAmounts(suite *DataValidationTestSuite, sc *types.StateChangeConnection, tokenID string) int64 {
	var total int64
	for _, edge := range sc.Edges {
		balanceChange, ok := edge.Node.(*types.BalanceChange)
		suite.Require().True(ok, "state change should be BalanceChange type")

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

// validateTrustlineAddedChange validates a trustline-added state change (TRUSTLINE/ADD). Exactly one of
// the asset token id / liquidity-pool id identifies the trustline, and the limit is set on ADD.
func validateTrustlineAddedChange(suite *DataValidationTestSuite, ta *types.TrustlineAddedChange, expectedTokenID, expectedLiquidityPoolID string) {
	suite.Require().NotNil(ta, "trustline added change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryTrustline, ta.GetCategory(), "should be TRUSTLINE category")
	suite.Require().Equal(types.StateChangeReasonAdd, ta.GetReason(), "reason mismatch")
	if expectedTokenID != "" {
		suite.Require().NotNil(ta.TokenID, "token ID should not be nil")
		suite.Require().Equal(expectedTokenID, *ta.TokenID, "token ID mismatch")
	}
	if expectedLiquidityPoolID != "" {
		suite.Require().NotNil(ta.LiquidityPoolID, "liquidity pool ID should not be nil")
		suite.Require().Equal(expectedLiquidityPoolID, *ta.LiquidityPoolID, "liquidity pool ID mismatch")
	}
	suite.Require().NotEmpty(ta.Limit, "limit should not be empty for ADD")
}

// validateTrustlineRemovedChange validates a trustline-removed state change (TRUSTLINE/REMOVE). Exactly
// one of the asset token id / liquidity-pool id identifies the trustline.
func validateTrustlineRemovedChange(suite *DataValidationTestSuite, tr *types.TrustlineRemovedChange, expectedTokenID, expectedLiquidityPoolID string) {
	suite.Require().NotNil(tr, "trustline removed change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryTrustline, tr.GetCategory(), "should be TRUSTLINE category")
	suite.Require().Equal(types.StateChangeReasonRemove, tr.GetReason(), "reason mismatch")
	if expectedTokenID != "" {
		suite.Require().NotNil(tr.TokenID, "token ID should not be nil")
		suite.Require().Equal(expectedTokenID, *tr.TokenID, "token ID mismatch")
	}
	if expectedLiquidityPoolID != "" {
		suite.Require().NotNil(tr.LiquidityPoolID, "liquidity pool ID should not be nil")
		suite.Require().Equal(expectedLiquidityPoolID, *tr.LiquidityPoolID, "liquidity pool ID mismatch")
	}
}

// validateBalanceAuthorizationChange validates a balance authorization state change. Flags are now a
// typed []TrustlineFlag list (nil for SAC contract-holder authorization, which has no trustline flags).
func validateBalanceAuthorizationChange(suite *DataValidationTestSuite, bac *types.BalanceAuthorizationChange,
	expectedReason types.StateChangeReason, expectedFlags []types.TrustlineFlag, expectedTokenID string,
) {
	suite.Require().NotNil(bac, "balance authorization change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryBalanceAuthorization, bac.GetCategory(), "should be BALANCE_AUTHORIZATION category")
	suite.Require().Equal(expectedReason, bac.GetReason(), "reason mismatch")
	suite.Require().Equal(len(expectedFlags), len(bac.Flags), "flags count mismatch")
	for _, expectedFlag := range expectedFlags {
		suite.Require().Contains(bac.Flags, expectedFlag, "expected flag not found: %s", expectedFlag)
	}
	if expectedTokenID != "" {
		suite.Require().NotNil(bac.TokenID, "token ID should not be nil")
		suite.Require().Equal(expectedTokenID, *bac.TokenID, "token ID mismatch")
	}
}

// validateAccountFlagsChange validates an account-flags state change (FLAGS/SET or FLAGS/CLEAR).
// Flags are now a typed []AccountFlag list.
func validateAccountFlagsChange(suite *DataValidationTestSuite, fc *types.AccountFlagsChange, expectedReason types.StateChangeReason, expectedFlags []types.AccountFlag) {
	suite.Require().NotNil(fc, "account flags change should not be nil")
	suite.Require().Equal(types.StateChangeCategoryFlags, fc.GetCategory(), "should be FLAGS category")
	suite.Require().Equal(expectedReason, fc.GetReason(), "reason mismatch")
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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation, ledgerNumber, types.OperationTypePayment)
	suite.Require().Equal(types.OperationTypePayment, operation.Type, "operation type should be PAYMENT")
}

func (suite *DataValidationTestSuite) validatePaymentStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	balanceCategory := types.StateChangeCategoryBalance
	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	secondaryAccount := suite.testEnv.SecondaryAccountKP.Address()

	// 3 state changes for this transaction: source debit + destination credit + the transaction-fee row
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 3, "should have exactly 3 state changes")

	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("secondary account: %s\n", secondaryAccount)

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
	sc := primaryStateChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, sc, xlmContractAddress, "100000000", types.StateChangeReasonDebit)

	// 1 CREDIT change for secondary account
	suite.Require().Len(secondaryStateChanges.Edges, 1, "should have exactly 1 state change for secondary account")
	sc = secondaryStateChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, sc, xlmContractAddress, "100000000", types.StateChangeReasonCredit)
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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
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
	balanceCategory := types.StateChangeCategoryBalance
	accountCategory := types.StateChangeCategoryAccount
	dataEntryCategory := types.StateChangeCategoryDataEntry
	signerCategory := types.StateChangeCategorySigner
	addReason := types.StateChangeReasonAdd
	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	sponsoredNewAccount := suite.testEnv.SponsoredNewAccountKP.Address()

	// Verify total count of state changes for this transaction
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 6, "should have exactly 6 total state changes")

	for i, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("State Change #%d:\n%s\n", i+1, string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("sponsored new account: %s\n", sponsoredNewAccount)
	fmt.Printf("xlm contract address: %s\n", xlmContractAddress)

	suite.Require().Len(stateChanges.Edges, 6, "should have exactly 6 total state changes")

	// Fetch all state changes in parallel
	sponsorshipQueries := []stateChangeQuery{
		{name: "primaryBalanceChange", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: nil},
		{name: "sponsoredBalanceChange", account: sponsoredNewAccount, txHash: &txHash, category: &balanceCategory, reason: nil},
		{name: "sponsoredAccountChange", account: sponsoredNewAccount, txHash: &txHash, category: &accountCategory, reason: nil},
		{name: "primaryDataEntryChange", account: primaryAccount, txHash: &txHash, category: &dataEntryCategory, reason: nil},
		{name: "sponsoredSignerChange", account: sponsoredNewAccount, txHash: &txHash, category: &signerCategory, reason: &addReason},
	}
	sponsorshipResults := suite.fetchStateChangesInParallel(ctx, sponsorshipQueries, &first)

	// Extract and validate results
	primaryBalanceChanges := sponsorshipResults["primaryBalanceChange"]
	sponsoredBalanceChanges := sponsorshipResults["sponsoredBalanceChange"]
	sponsoredAccountChanges := sponsorshipResults["sponsoredAccountChange"]
	primaryDataEntryChanges := sponsorshipResults["primaryDataEntryChange"]
	sponsoredSignerChanges := sponsorshipResults["sponsoredSignerChange"]

	// Validate all results are not nil
	suite.Require().NotNil(primaryBalanceChanges, "primary balance changes should not be nil")
	suite.Require().NotNil(sponsoredBalanceChanges, "sponsored balance changes should not be nil")
	suite.Require().NotNil(sponsoredAccountChanges, "sponsored account changes should not be nil")
	suite.Require().NotNil(primaryDataEntryChanges, "primary data entry changes should not be nil")
	suite.Require().NotNil(sponsoredSignerChanges, "sponsored signer changes should not be nil")

	// 1 BALANCE/DEBIT change for primary account (sending starting balance)
	suite.Require().Len(primaryBalanceChanges.Edges, 1, "should have exactly 1 BALANCE/DEBIT balance change for primary account")
	balanceChange := primaryBalanceChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, balanceChange, xlmContractAddress, "50000000", types.StateChangeReasonDebit)

	// 1 BALANCE/CREDIT change for sponsored account (receiving starting balance)
	suite.Require().Len(sponsoredBalanceChanges.Edges, 1, "should have exactly 1 BALANCE/CREDIT balance change for sponsored account")
	balanceChange = sponsoredBalanceChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, balanceChange, xlmContractAddress, "50000000", types.StateChangeReasonCredit)

	// 1 ACCOUNT/CREATE account change for sponsored account
	suite.Require().Len(sponsoredAccountChanges.Edges, 1, "should have exactly 1 ACCOUNT/CREATE account change")
	accountChange := sponsoredAccountChanges.Edges[0].Node.(*types.AccountCreatedChange)
	validateAccountCreatedChange(suite, accountChange, primaryAccount)

	// 1 DATA_ENTRY/ADD data entry change for primary account
	suite.Require().Len(primaryDataEntryChanges.Edges, 1, "should have exactly 1 DATA_ENTRY/ADD data entry change for primary account")
	dataEntryChange := primaryDataEntryChanges.Edges[0].Node.(*types.DataEntryAddedChange)
	validateDataEntryAddedChange(suite, dataEntryChange, "foo", "bar")

	// 1 SIGNER/ADD change for sponsored account with default signer weight = 1
	suite.Require().Len(sponsoredSignerChanges.Edges, 1, "should have exactly 1 SIGNER/CREATE signer change for sponsored account")
	signerChange := sponsoredSignerChanges.Edges[0].Node.(*types.SignerAddedChange)
	validateSignerAddedChange(suite, signerChange, sponsoredNewAccount, 1)
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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
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
		suite.Require().Equal(expectedOpTypes[i], edge.Node.Type, "operation type mismatch at index %d", i)
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
	balanceCategory := types.StateChangeCategoryBalance
	trustlineCategory := types.StateChangeCategoryTrustline
	balanceAuthCategory := types.StateChangeCategoryBalanceAuthorization
	mintReason := types.StateChangeReasonMint
	burnReason := types.StateChangeReasonBurn
	creditReason := types.StateChangeReasonCredit
	debitReason := types.StateChangeReasonDebit
	setReason := types.StateChangeReasonSet

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 26, "should have exactly 25 state changes")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("secondary account: %s\n", secondaryAccount)
	fmt.Printf("test2 contract address: %s\n", test2ContractAddress)
	fmt.Printf("xlm contract address: %s\n", xlmContractAddress)

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
		switch edge.Node.GetReason() {
		case types.StateChangeReasonAdd:
			validateTrustlineAddedChange(suite, edge.Node.(*types.TrustlineAddedChange), test2ContractAddress, "")
			foundAdd = true
		case types.StateChangeReasonRemove:
			validateTrustlineRemovedChange(suite, edge.Node.(*types.TrustlineRemovedChange), test2ContractAddress, "")
			foundRemove = true
		default:
			suite.Require().Failf("unexpected trustline change reason", "reason %s", edge.Node.GetReason())
		}
	}
	suite.Require().True(foundAdd, "should have ADD trustline change")
	suite.Require().True(foundRemove, "should have REMOVE trustline change")

	// 3b. BALANCE_AUTHORIZATION Changes: Secondary should have exactly 1 (SET with authorized flag)
	suite.Require().Len(authChanges.Edges, 1, "should have exactly 1 BALANCE_AUTHORIZATION/SET change")
	authChange := authChanges.Edges[0].Node.(*types.BalanceAuthorizationChange)
	validateBalanceAuthorizationChange(suite, authChange, types.StateChangeReasonSet, []types.TrustlineFlag{types.TrustlineFlagAuthorized}, test2ContractAddress)

	// 4. SPECIFIC BALANCE CHANGE VALIDATIONS
	// 4a. Validate MINT changes have correct token ID and account
	for _, edge := range mintChanges.Edges {
		bc := edge.Node.(*types.BalanceChange)
		suite.Require().Equal(test2ContractAddress, bc.TokenID, "MINT token should be TEST2")
		suite.Require().NotEmpty(bc.Amount, "MINT amount should not be empty")
	}

	// 4b. Validate BURN changes have correct token ID and account
	for _, edge := range burnChanges.Edges {
		bc := edge.Node.(*types.BalanceChange)
		suite.Require().Equal(test2ContractAddress, bc.TokenID, "BURN token should be TEST2")
		suite.Require().NotEmpty(bc.Amount, "BURN amount should not be empty")
	}

	// 4c. Validate CREDIT changes have correct token ID and account
	tokenSet := set.NewSet(test2ContractAddress, xlmContractAddress)
	for _, edge := range creditChanges.Edges {
		bc := edge.Node.(*types.BalanceChange)
		suite.Require().True(tokenSet.Contains(bc.TokenID), "CREDIT token should be TEST2 or XLM")
		suite.Require().NotEmpty(bc.Amount, "CREDIT amount should not be empty")
	}

	// 4d. Validate DEBIT changes have correct token ID and account
	for _, edge := range debitChanges.Edges {
		bc := edge.Node.(*types.BalanceChange)
		suite.Require().True(tokenSet.Contains(bc.TokenID), "DEBIT token should be TEST2 or XLM")
		suite.Require().NotEmpty(bc.Amount, "DEBIT amount should not be empty")
	}
}

func (suite *DataValidationTestSuite) TestAuthRequiredOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating auth-required issuer setup operations data...")

	// Transaction 1: Issuer Setup - Find the issuer setup use case
	issuerSetupUseCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/authRequiredIssuerSetupOps")
	suite.Require().NotNil(issuerSetupUseCase, "authRequiredIssuerSetupOps use case not found")
	suite.Require().NotEmpty(issuerSetupUseCase.GetTransactionResult.Hash, "issuer setup transaction hash should not be empty")

	issuerSetupTxHash := issuerSetupUseCase.GetTransactionResult.Hash
	issuerSetupTx := validateTransactionBase(suite, ctx, issuerSetupTxHash)
	suite.validateAuthRequiredIssuerSetupOperations(ctx, issuerSetupTxHash, int64(issuerSetupTx.LedgerNumber))
	suite.validateAuthRequiredIssuerSetupStateChanges(ctx, issuerSetupTxHash, int64(issuerSetupTx.LedgerNumber))

	log.Ctx(ctx).Info("🔍 Validating auth-required asset operations data...")

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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeSetOptions,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.Type, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateAuthRequiredAssetOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
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
		suite.Require().Equal(expectedOpTypes[i], edge.Node.Type, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateAuthRequiredIssuerSetupStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)

	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()

	// Define filter constants
	flagsCategory := types.StateChangeCategoryFlags
	setReason := types.StateChangeReasonSet

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 1 state change")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)

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

	expectedFlags := []types.AccountFlag{types.AccountFlagAuthRequired, types.AccountFlagAuthRevocable, types.AccountFlagAuthClawbackEnabled}
	flagsSetChange := flagsSetPrimary.Edges[0].Node.(*types.AccountFlagsChange)
	validateStateChangeBase(suite, flagsSetChange, ledgerNumber)
	validateAccountFlagsChange(suite, flagsSetChange, types.StateChangeReasonSet, expectedFlags)
}

func (suite *DataValidationTestSuite) validateAuthRequiredAssetStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(15)

	// Setup: Compute expected values from fixtures
	test1Asset := xdr.MustNewCreditAsset("TEST1", suite.testEnv.PrimaryAccountKP.Address())
	test1ContractAddress := suite.getAssetContractAddress(test1Asset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	secondaryAccount := suite.testEnv.SecondaryAccountKP.Address()

	// Define filter constants
	balanceCategory := types.StateChangeCategoryBalance
	trustlineCategory := types.StateChangeCategoryTrustline
	balanceAuthCategory := types.StateChangeCategoryBalanceAuthorization
	setReason := types.StateChangeReasonSet
	clearReason := types.StateChangeReasonClear
	addReason := types.StateChangeReasonAdd
	removeReason := types.StateChangeReasonRemove
	mintReason := types.StateChangeReasonMint
	burnReason := types.StateChangeReasonBurn
	creditReason := types.StateChangeReasonCredit
	debitReason := types.StateChangeReasonDebit

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 10, "should have exactly 9 state changes")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("secondary account: %s\n", secondaryAccount)
	fmt.Printf("test1 contract address: %s\n", test1ContractAddress)

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
	validateBalanceAuthorizationChange(suite, authSetSecondaryClawback, types.StateChangeReasonSet, []types.TrustlineFlag{types.TrustlineFlagClawbackEnabled}, test1ContractAddress)

	// Second SET change: authorized flag from SetTrustLineFlags
	authSetSecondaryAuthorized := balanceAuthSetSecondary.Edges[1].Node.(*types.BalanceAuthorizationChange)
	validateBalanceAuthorizationChange(suite, authSetSecondaryAuthorized, types.StateChangeReasonSet, []types.TrustlineFlag{types.TrustlineFlagAuthorized}, test1ContractAddress)

	// Secondary account: BALANCE_AUTHORIZATION/CLEAR with "authorized" flag
	suite.Require().Len(balanceAuthClearSecondary.Edges, 1, "should have exactly 1 BALANCE_AUTHORIZATION/CLEAR for secondary")
	authClearSecondary := balanceAuthClearSecondary.Edges[0].Node.(*types.BalanceAuthorizationChange)
	validateBalanceAuthorizationChange(suite, authClearSecondary, types.StateChangeReasonClear, []types.TrustlineFlag{types.TrustlineFlagAuthorized}, test1ContractAddress)

	// 5. TRUSTLINE STATE CHANGES VALIDATION FOR SECONDARY ACCOUNT
	suite.Require().Len(trustlineAdd.Edges, 1, "should have exactly 1 TRUSTLINE/ADD")
	suite.Require().Len(trustlineRemove.Edges, 1, "should have exactly 1 TRUSTLINE/REMOVE")

	trustlineAddChange := trustlineAdd.Edges[0].Node.(*types.TrustlineAddedChange)
	validateTrustlineAddedChange(suite, trustlineAddChange, test1ContractAddress, "")

	trustlineRemoveChange := trustlineRemove.Edges[0].Node.(*types.TrustlineRemovedChange)
	validateTrustlineRemovedChange(suite, trustlineRemoveChange, test1ContractAddress, "")

	// 6. BALANCE STATE CHANGES VALIDATION
	// Validate counts
	suite.Require().Len(balanceMint.Edges, 1, "should have exactly 1 BALANCE/MINT")
	suite.Require().Len(balanceCredit.Edges, 1, "should have exactly 1 BALANCE/CREDIT")
	suite.Require().Len(balanceBurn.Edges, 1, "should have exactly 1 BALANCE/BURN")
	suite.Require().Len(balanceDebit.Edges, 1, "should have exactly 1 BALANCE/DEBIT")

	// Validate MINT
	mintChange := balanceMint.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, mintChange, test1ContractAddress, "10000000000", types.StateChangeReasonMint)

	// Validate CREDIT
	creditChange := balanceCredit.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, creditChange, test1ContractAddress, "10000000000", types.StateChangeReasonCredit)

	// Validate BURN
	burnChange := balanceBurn.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, burnChange, test1ContractAddress, "10000000000", types.StateChangeReasonBurn)

	// Validate DEBIT (from clawback)
	debitChange := balanceDebit.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, debitChange, test1ContractAddress, "10000000000", types.StateChangeReasonDebit)

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
	log.Ctx(ctx).Info("🔍 Validating account merge operations data...")

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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation, ledgerNumber, types.OperationTypeAccountMerge)
	suite.Require().Equal(types.OperationTypeAccountMerge, operation.Type, "operation type should be ACCOUNT_MERGE")
}

func (suite *DataValidationTestSuite) validateAccountMergeStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	accountCategory := types.StateChangeCategoryAccount
	balanceCategory := types.StateChangeCategoryBalance
	mergeReason := types.StateChangeReasonMerge
	creditReason := types.StateChangeReasonCredit
	debitReason := types.StateChangeReasonDebit
	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	sponsoredNewAccount := suite.testEnv.SponsoredNewAccountKP.Address()

	// Verify total count of state changes for this transaction
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 4, "should have exactly 4 state changes")

	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("sponsored new account: %s\n", sponsoredNewAccount)
	fmt.Printf("xlm contract address: %s\n", xlmContractAddress)

	// Fetch state changes in parallel
	accountMergeQueries := []stateChangeQuery{
		{name: "accountMerge", account: sponsoredNewAccount, txHash: &txHash, category: &accountCategory, reason: &mergeReason},
		{name: "balanceCredit", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &creditReason},
		{name: "balanceDebit", account: sponsoredNewAccount, txHash: &txHash, category: &balanceCategory, reason: &debitReason},
	}
	accountMergeResults := suite.fetchStateChangesInParallel(ctx, accountMergeQueries, &first)

	// Extract results
	accountMergeChanges := accountMergeResults["accountMerge"]
	balanceCreditChanges := accountMergeResults["balanceCredit"]
	balanceDebitChanges := accountMergeResults["balanceDebit"]

	// Validate results are not nil
	suite.Require().NotNil(accountMergeChanges, "ACCOUNT/MERGE changes should not be nil")
	suite.Require().NotNil(balanceCreditChanges, "BALANCE/CREDIT changes should not be nil")
	suite.Require().NotNil(balanceDebitChanges, "BALANCE/DEBIT changes should not be nil")

	// Validate ACCOUNT/MERGE change
	suite.Require().Len(accountMergeChanges.Edges, 1, "should have exactly 1 ACCOUNT/MERGE change")
	accountChange := accountMergeChanges.Edges[0].Node.(*types.AccountMergedChange)
	suite.Require().Equal(types.StateChangeCategoryAccount, accountChange.GetCategory(), "should be ACCOUNT category")
	suite.Require().Equal(types.StateChangeReasonMerge, accountChange.GetReason(), "reason should be MERGE")
	suite.Require().NotEmpty(accountChange.DestinationAddress, "destination address should not be empty")
	suite.Require().Equal(primaryAccount, accountChange.DestinationAddress, "destination address should be the merge destination")

	// Validate BALANCE/CREDIT change
	suite.Require().Len(balanceCreditChanges.Edges, 1, "should have exactly 1 BALANCE/CREDIT change")
	balanceCreditChange := balanceCreditChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, balanceCreditChange, xlmContractAddress, "50000000", types.StateChangeReasonCredit)

	// Validate BALANCE/DEBIT change
	suite.Require().Len(balanceDebitChanges.Edges, 1, "should have exactly 1 BALANCE/DEBIT change")
	balanceDebitChange := balanceDebitChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, balanceDebitChange, xlmContractAddress, "50000000", types.StateChangeReasonDebit)
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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeInvokeHostFunction,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.Type, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateInvokeContractStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(15)

	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	balanceCategory := types.StateChangeCategoryBalance
	creditReason := types.StateChangeReasonCredit
	debitReason := types.StateChangeReasonDebit

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 3, "should have exactly 11 state changes")

	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("xlm contract address: %s\n", xlmContractAddress)

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
	balanceCreditChange := balanceCreditChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, balanceCreditChange, xlmContractAddress, "100000000", types.StateChangeReasonCredit)

	// Validate BALANCE/DEBIT change
	suite.Require().Len(balanceDebitChanges.Edges, 1, "should have exactly 1 BALANCE/DEBIT change")
	balanceDebitChange := balanceDebitChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, balanceDebitChange, xlmContractAddress, "100000000", types.StateChangeReasonDebit)
}

func (suite *DataValidationTestSuite) TestDeployContractOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating contract-deploy operation data...")

	useCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Soroban/deployContractOp")
	suite.Require().NotNil(useCase, "deployContractOp use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)

	// The deploy change's account is the contract ID, not the deployer, so fetch by tx hash.
	first := int32(15)
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")

	// Validate base fields on every change and locate the ACCOUNT/CREATE deploy change in one pass.
	var deployChange *types.AccountCreatedChange
	for _, edge := range stateChanges.Edges {
		validateStateChangeBase(suite, edge.Node, int64(tx.LedgerNumber))
		if cd, ok := edge.Node.(*types.AccountCreatedChange); ok {
			deployChange = cd
		}
	}
	suite.Require().NotNil(deployChange, "expected an ACCOUNT/CREATE contract-deploy state change")
	validateAccountCreatedChange(suite, deployChange, suite.testEnv.PrimaryAccountKP.Address())
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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
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
		suite.Require().Equal(expectedOpTypes[i], edge.Node.Type, "operation type mismatch at index %d", i)
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
	trustlineCategory := types.StateChangeCategoryTrustline
	balanceAuthCategory := types.StateChangeCategoryBalanceAuthorization
	balanceCategory := types.StateChangeCategoryBalance
	setReason := types.StateChangeReasonSet
	addReason := types.StateChangeReasonAdd
	mintReason := types.StateChangeReasonMint

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("secondary account: %s\n", secondaryAccount)
	fmt.Printf("test3 contract address: %s\n", test3ContractAddress)

	// 2. FETCH STATE CHANGES IN PARALLEL
	claimableBalanceQueries := []stateChangeQuery{
		{name: "trustlineAdd", account: secondaryAccount, txHash: &txHash, category: &trustlineCategory, reason: &addReason},
		{name: "balanceAuthSet", account: secondaryAccount, txHash: &txHash, category: &balanceAuthCategory, reason: &setReason},
		{name: "balanceMint", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &mintReason},
	}
	claimableBalanceResults := suite.fetchStateChangesInParallel(ctx, claimableBalanceQueries, &first)

	// Extract results
	trustlineAdd := claimableBalanceResults["trustlineAdd"]
	balanceAuthSet := claimableBalanceResults["balanceAuthSet"]
	balanceMint := claimableBalanceResults["balanceMint"]

	// Validate results are not nil
	suite.Require().NotNil(trustlineAdd, "TRUSTLINE/ADD should not be nil")
	suite.Require().NotNil(balanceAuthSet, "BALANCE_AUTHORIZATION/SET should not be nil")
	suite.Require().NotNil(balanceMint, "BALANCE/MINT should not be nil")

	// 3. TRUSTLINE STATE CHANGES VALIDATION FOR SECONDARY ACCOUNT
	suite.Require().Len(trustlineAdd.Edges, 1, "should have exactly 1 TRUSTLINE/ADD")
	trustlineAddChange := trustlineAdd.Edges[0].Node.(*types.TrustlineAddedChange)
	validateTrustlineAddedChange(suite, trustlineAddChange, test3ContractAddress, "")

	// 4. BALANCE_AUTHORIZATION STATE CHANGES VALIDATION
	// Secondary account should have 2 BALANCE_AUTHORIZATION/SET changes:
	// - One with clawback_enabled flag (from trustline creation inheriting issuer's clawback flag)
	// - One with authorized flag (from SetTrustLineFlags operation)
	suite.Require().Len(balanceAuthSet.Edges, 2, "should have exactly 2 BALANCE_AUTHORIZATION/SET for secondary")
	authSetSecondary := balanceAuthSet.Edges[0].Node.(*types.BalanceAuthorizationChange)
	validateBalanceAuthorizationChange(suite, authSetSecondary, types.StateChangeReasonSet, []types.TrustlineFlag{types.TrustlineFlagClawbackEnabled}, test3ContractAddress)

	// Second SET change: authorized flag from SetTrustLineFlags
	authSetSecondaryAuthorized := balanceAuthSet.Edges[1].Node.(*types.BalanceAuthorizationChange)
	validateBalanceAuthorizationChange(suite, authSetSecondaryAuthorized, types.StateChangeReasonSet, []types.TrustlineFlag{types.TrustlineFlagAuthorized}, test3ContractAddress)

	// 5. BALANCE STATE CHANGES VALIDATION - 2 claimable balances are created
	suite.Require().Len(balanceMint.Edges, 2, "should have exactly 2 BALANCE/MINT")
	for _, edge := range balanceMint.Edges {
		mintChange := edge.Node.(*types.BalanceChange)
		validateBalanceChange(suite, mintChange, test3ContractAddress, "10000000", types.StateChangeReasonMint)
	}
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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
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
	balanceCategory := types.StateChangeCategoryBalance
	creditReason := types.StateChangeReasonCredit

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 2 state changes")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("secondary account: %s\n", secondaryAccount)
	fmt.Printf("test3 contract address: %s\n", test3ContractAddress)

	// 2. FETCH BALANCE/CREDIT STATE CHANGE
	claimQueries := []stateChangeQuery{
		{name: "balanceCredit", account: secondaryAccount, txHash: &txHash, category: &balanceCategory, reason: &creditReason},
	}
	claimResults := suite.fetchStateChangesInParallel(ctx, claimQueries, &first)

	// Extract and validate results
	balanceCreditChanges := claimResults["balanceCredit"]
	suite.Require().NotNil(balanceCreditChanges, "BALANCE/CREDIT changes should not be nil")

	// 3. VALIDATE BALANCE/CREDIT CHANGE
	suite.Require().Len(balanceCreditChanges.Edges, 1, "should have exactly 1 BALANCE/CREDIT change")
	balanceCreditChange := balanceCreditChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, balanceCreditChange, test3ContractAddress, "10000000", types.StateChangeReasonCredit)
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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	operation := operations.Edges[0].Node
	validateOperationBase(suite, operation, ledgerNumber, types.OperationTypeClawbackClaimableBalance)
	suite.Require().Equal(types.OperationTypeClawbackClaimableBalance, operation.Type, "operation type should be CLAWBACK_CLAIMABLE_BALANCE")
}

func (suite *DataValidationTestSuite) validateClawbackClaimableBalanceStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)

	// Setup: Compute expected values from fixtures
	test3Asset := xdr.MustNewCreditAsset("TEST3", suite.testEnv.PrimaryAccountKP.Address())
	test3ContractAddress := suite.getAssetContractAddress(test3Asset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()

	// Define filter constants
	balanceCategory := types.StateChangeCategoryBalance
	burnReason := types.StateChangeReasonBurn

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 2 state changes")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("test3 contract address: %s\n", test3ContractAddress)

	// 2. FETCH BALANCE/BURN STATE CHANGE
	clawbackQueries := []stateChangeQuery{
		{name: "balanceBurn", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &burnReason},
	}
	clawbackResults := suite.fetchStateChangesInParallel(ctx, clawbackQueries, &first)

	// Extract and validate results
	balanceBurnChanges := clawbackResults["balanceBurn"]
	suite.Require().NotNil(balanceBurnChanges, "BALANCE/BURN changes should not be nil")

	// 3. VALIDATE BALANCE/BURN CHANGE
	suite.Require().Len(balanceBurnChanges.Edges, 1, "should have exactly 1 BALANCE/BURN change")
	balanceBurnChange := balanceBurnChanges.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, balanceBurnChange, test3ContractAddress, "10000000", types.StateChangeReasonBurn)
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
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 1, "should have exactly 1 operation")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeSetOptions,
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.Type, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateClearAuthFlagsStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)

	// Setup: Get primary account
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()

	// Define filter constants
	flagsCategory := types.StateChangeCategoryFlags
	clearReason := types.StateChangeReasonClear

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 2, "should have exactly 1 state change")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)

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
	expectedFlags := []types.AccountFlag{types.AccountFlagAuthRequired, types.AccountFlagAuthRevocable, types.AccountFlagAuthClawbackEnabled}
	flagsClearChange := flagsClearPrimary.Edges[0].Node.(*types.AccountFlagsChange)
	validateAccountFlagsChange(suite, flagsClearChange, types.StateChangeReasonClear, expectedFlags)
}

func (suite *DataValidationTestSuite) TestLiquidityPoolOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating liquidity pool operations data...")

	// Find the liquidity pool use case
	useCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/liquidityPoolOps")
	suite.Require().NotNil(useCase, "liquidityPoolOps use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validateLiquidityPoolOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validateLiquidityPoolStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateLiquidityPoolOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 4, "should have exactly 4 operations")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeChangeTrust,           // Create trustline to pool
		types.OperationTypeLiquidityPoolDeposit,  // Deposit into pool
		types.OperationTypeLiquidityPoolWithdraw, // Withdraw from pool
		types.OperationTypeChangeTrust,           // Remove trustline to pool
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.Type, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateLiquidityPoolStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(30)

	// Setup: Compute expected values from fixtures
	test2Asset := xdr.MustNewCreditAsset("TEST2", suite.testEnv.PrimaryAccountKP.Address())
	test2ContractAddress := suite.getAssetContractAddress(test2Asset)
	xlmContractAddress := suite.getAssetContractAddress(xlmAsset)
	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 8, "should have exactly 7 state changes")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primary account: %s\n", primaryAccount)
	fmt.Printf("test2 contract address: %s\n", test2ContractAddress)
	fmt.Printf("xlm contract address: %s\n", xlmContractAddress)

	// 2. VALIDATE PRESENCE OF KEY STATE CHANGE CATEGORIES
	balanceCategory := types.StateChangeCategoryBalance
	trustlineCategory := types.StateChangeCategoryTrustline
	balanceAuthCategory := types.StateChangeCategoryBalanceAuthorization
	addReason := types.StateChangeReasonAdd
	removeReason := types.StateChangeReasonRemove
	setReason := types.StateChangeReasonSet
	debitReason := types.StateChangeReasonDebit
	creditReason := types.StateChangeReasonCredit
	mintReason := types.StateChangeReasonMint
	burnReason := types.StateChangeReasonBurn

	// Fetch state changes for validation
	lpQueries := []stateChangeQuery{
		{name: "trustlineAdd", account: primaryAccount, txHash: &txHash, category: &trustlineCategory, reason: &addReason},
		{name: "trustlineRemove", account: primaryAccount, txHash: &txHash, category: &trustlineCategory, reason: &removeReason},
		{name: "balanceAuthSet", account: primaryAccount, txHash: &txHash, category: &balanceAuthCategory, reason: &setReason},
		{name: "balanceDebit", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &debitReason},
		{name: "balanceCredit", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &creditReason},
		{name: "balanceMint", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &mintReason},
		{name: "balanceBurn", account: primaryAccount, txHash: &txHash, category: &balanceCategory, reason: &burnReason},
	}
	lpResults := suite.fetchStateChangesInParallel(ctx, lpQueries, &first)

	// Extract results
	trustlineAdd := lpResults["trustlineAdd"]
	trustlineRemove := lpResults["trustlineRemove"]
	balanceAuthSet := lpResults["balanceAuthSet"]
	balanceDebit := lpResults["balanceDebit"]
	balanceCredit := lpResults["balanceCredit"]
	balanceMint := lpResults["balanceMint"]
	balanceBurn := lpResults["balanceBurn"]

	// Validate results are not nil
	suite.Require().NotNil(trustlineAdd, "TRUSTLINE/ADD should not be nil")
	suite.Require().NotNil(trustlineRemove, "TRUSTLINE/REMOVE should not be nil")
	suite.Require().NotNil(balanceAuthSet, "BALANCE_AUTHORIZATION/SET should not be nil")
	suite.Require().NotNil(balanceDebit, "BALANCE/DEBIT should not be nil")
	suite.Require().NotNil(balanceCredit, "BALANCE/CREDIT should not be nil")
	suite.Require().NotNil(balanceMint, "BALANCE/MINT should not be nil")
	suite.Require().NotNil(balanceBurn, "BALANCE/BURN should not be nil")

	// 3. BALANCE_AUTHORIZATION VALIDATION
	// LP trustline should have exactly 1 BALANCE_AUTHORIZATION/SET with empty flags, null tokenId and pool ID
	suite.Require().Len(balanceAuthSet.Edges, 1, "should have exactly 1 BALANCE_AUTHORIZATION/SET for liquidity pool")
	balanceAuth := balanceAuthSet.Edges[0].Node.(*types.BalanceAuthorizationChange)
	suite.Require().Equal(suite.testEnv.LiquidityPoolID, *balanceAuth.LiquidityPoolID, "balance auth change liquidity pool ID does not match")
	validateBalanceAuthorizationChange(suite, balanceAuth, types.StateChangeReasonSet, []types.TrustlineFlag{}, "")

	// 4. TRUSTLINE VALIDATION
	// LP trustlines should have null tokenId and pool ID in liquidityPoolId
	suite.Require().Len(trustlineAdd.Edges, 1, "should have exactly 1 TRUSTLINE/ADD for liquidity pool")
	trustlineAddChange := trustlineAdd.Edges[0].Node.(*types.TrustlineAddedChange)
	validateTrustlineAddedChange(suite, trustlineAddChange, "", suite.testEnv.LiquidityPoolID)

	suite.Require().Len(trustlineRemove.Edges, 1, "should have exactly 1 TRUSTLINE/REMOVE for liquidity pool")
	trustlineRemoveChange := trustlineRemove.Edges[0].Node.(*types.TrustlineRemovedChange)
	validateTrustlineRemovedChange(suite, trustlineRemoveChange, "", suite.testEnv.LiquidityPoolID)

	// 5. BALANCE CHANGES VALIDATION
	// DEBIT: XLM deposited into pool (amount = 1000000000)
	suite.Require().Len(balanceDebit.Edges, 1, "should have exactly 1 BALANCE/DEBIT")
	debitChange := balanceDebit.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, debitChange, xlmContractAddress, "1000000000", types.StateChangeReasonDebit)

	// CREDIT: XLM withdrawn from pool (amount = 1000000000)
	suite.Require().Len(balanceCredit.Edges, 1, "should have exactly 1 BALANCE/CREDIT")
	creditChange := balanceCredit.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, creditChange, xlmContractAddress, "1000000000", types.StateChangeReasonCredit)

	// MINT: TEST2 minted to LP (amount = 1000000000)
	suite.Require().Len(balanceMint.Edges, 1, "should have exactly 1 BALANCE/MINT")
	mintChange := balanceMint.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, mintChange, test2ContractAddress, "1000000000", types.StateChangeReasonMint)

	// BURN: TEST2 burned from LP back to issuer (amount = 1000000000)
	suite.Require().Len(balanceBurn.Edges, 1, "should have exactly 1 BALANCE/BURN")
	burnChange := balanceBurn.Edges[0].Node.(*types.BalanceChange)
	validateBalanceChange(suite, burnChange, test2ContractAddress, "1000000000", types.StateChangeReasonBurn)
}

func (suite *DataValidationTestSuite) TestRevokeSponsorshipOpsDataValidation() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Validating revoke sponsorship operations data...")

	// Find the revoke sponsorship use case
	useCase := infrastructure.FindUseCase(suite.testEnv.UseCases, "Stellarclassic/revokeSponsorshipOps")
	suite.Require().NotNil(useCase, "revokeSponsorshipOps use case not found")
	suite.Require().NotEmpty(useCase.GetTransactionResult.Hash, "transaction hash should not be empty")

	txHash := useCase.GetTransactionResult.Hash
	tx := validateTransactionBase(suite, ctx, txHash)
	suite.validateRevokeSponsorshipOperations(ctx, txHash, int64(tx.LedgerNumber))
	suite.validateRevokeSponsorshipStateChanges(ctx, txHash, int64(tx.LedgerNumber))
}

func (suite *DataValidationTestSuite) validateRevokeSponsorshipOperations(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(10)
	operations, err := suite.testEnv.WBClient.GetTransactionOperations(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction operations")
	suite.Require().NotNil(operations, "operations should not be nil")
	suite.Require().Len(operations.Edges, 5, "should have exactly 5 operations")

	expectedOpTypes := []types.OperationType{
		types.OperationTypeBeginSponsoringFutureReserves, // Begin sponsorship
		types.OperationTypeManageData,                    // Create sponsored data entry
		types.OperationTypeEndSponsoringFutureReserves,   // End sponsorship
		types.OperationTypeRevokeSponsorship,             // Revoke sponsorship
		types.OperationTypeManageData,                    // Remove data entry
	}

	for i, edge := range operations.Edges {
		validateOperationBase(suite, edge.Node, ledgerNumber, expectedOpTypes[i])
		suite.Require().Equal(expectedOpTypes[i], edge.Node.Type, "operation type mismatch at index %d", i)
	}
}

func (suite *DataValidationTestSuite) validateRevokeSponsorshipStateChanges(ctx context.Context, txHash string, ledgerNumber int64) {
	first := int32(20)

	primaryAccount := suite.testEnv.PrimaryAccountKP.Address()
	secondaryAccount := suite.testEnv.SecondaryAccountKP.Address()

	// Define filter constants
	dataEntryCategory := types.StateChangeCategoryDataEntry
	addReason := types.StateChangeReasonAdd
	removeReason := types.StateChangeReasonRemove

	// 1. TOTAL STATE CHANGE COUNT VALIDATION
	stateChanges, err := suite.testEnv.WBClient.GetTransactionStateChanges(ctx, txHash, &wbclient.Page{First: &first})
	suite.Require().NoError(err, "failed to get transaction state changes")
	suite.Require().NotNil(stateChanges, "state changes should not be nil")
	suite.Require().Len(stateChanges.Edges, 3, "should have exactly 3 state changes")

	// Validate base fields for all state changes
	for _, edge := range stateChanges.Edges {
		jsonBytes, err := json.MarshalIndent(edge.Node, "", "  ")
		suite.Require().NoError(err, "failed to marshal state change")
		fmt.Printf("%s\n", string(jsonBytes))
		validateStateChangeBase(suite, edge.Node, ledgerNumber)
	}
	fmt.Printf("primaryAccount: %s\n", primaryAccount)
	fmt.Printf("secondaryAccount: %s\n", secondaryAccount)

	// 2. FETCH STATE CHANGES IN PARALLEL
	revokeSponsorshipQueries := []stateChangeQuery{
		{name: "dataEntryAdded", account: secondaryAccount, txHash: &txHash, category: &dataEntryCategory, reason: &addReason},
		{name: "dataEntryRemoved", account: secondaryAccount, txHash: &txHash, category: &dataEntryCategory, reason: &removeReason},
	}
	revokeSponsorshipResults := suite.fetchStateChangesInParallel(ctx, revokeSponsorshipQueries, &first)

	// Extract results
	dataEntryAdded := revokeSponsorshipResults["dataEntryAdded"]
	dataEntryRemoved := revokeSponsorshipResults["dataEntryRemoved"]

	// Validate results are not nil
	suite.Require().NotNil(dataEntryAdded, "DATA_ENTRY/ADD should not be nil")
	suite.Require().NotNil(dataEntryRemoved, "DATA_ENTRY/REMOVE should not be nil")

	// 4. DATA ENTRY STATE CHANGES VALIDATION

	// The sponsored data entry is created and then removed within the same transaction.
	suite.Require().Len(dataEntryAdded.Edges, 1, "should have exactly 1 DATA_ENTRY/ADD change")
	added := dataEntryAdded.Edges[0].Node.(*types.DataEntryAddedChange)
	validateDataEntryAddedChange(suite, added, "sponsored_data", "test_value")
	suite.Require().Len(dataEntryRemoved.Edges, 1, "should have exactly 1 DATA_ENTRY/REMOVE change")
	removed := dataEntryRemoved.Edges[0].Node.(*types.DataEntryRemovedChange)
	validateDataEntryRemovedChange(suite, removed, "sponsored_data", "test_value")
}
