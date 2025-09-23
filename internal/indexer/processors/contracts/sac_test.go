package contracts

import (
	"context"
	"testing"

	"github.com/stellar/go/keypair"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestSACEventsProcessor_ProcessOperation(t *testing.T) {
	processor := NewSACEventsProcessor(networkPassphrase)

	t.Run("V3 Format - set_authorized = true (was unauthorized)", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)

		// Test case: account was not authorized, had maintain liabilities flag
		tx := createTxWithTrustlineChanges(account, admin, asset, true, false, true, 3)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, stateChanges, 2) // Should create 2 state changes for authorization flags

		// First state change: Set AUTHORIZED_FLAG (was not set before)
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonSet,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedFlagName}, stateChanges[0].Flags)

		// Second state change: Remove AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG (was set before)
		assertContractEvent(t, stateChanges[1], types.StateChangeReasonClear,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedToMaintainLiabilitesFlagName}, stateChanges[1].Flags)
	})

	t.Run("V4 Format - set_authorized = true (was unauthorized)", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)

		// Test case: account was not authorized, had maintain liabilities flag
		tx := createTxWithTrustlineChanges(account, admin, asset, true, false, true, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, stateChanges, 2) // Should create 2 state changes for authorization flags

		// First state change: Set AUTHORIZED_FLAG
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonSet,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedFlagName}, stateChanges[0].Flags)

		// Second state change: Remove AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG
		assertContractEvent(t, stateChanges[1], types.StateChangeReasonClear,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedToMaintainLiabilitesFlagName}, stateChanges[1].Flags)
	})

	t.Run("V4 Format - set_authorized = false (was authorized)", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)

		// Test case: account was authorized, no maintain liabilities flag
		tx := createTxWithTrustlineChanges(account, admin, asset, false, true, false, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, stateChanges, 2) // Should create 2 state changes for authorization flags

		// First state change: Remove AUTHORIZED_FLAG
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonClear,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedFlagName}, stateChanges[0].Flags)

		// Second state change: Set AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG
		assertContractEvent(t, stateChanges[1], types.StateChangeReasonSet,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedToMaintainLiabilitesFlagName}, stateChanges[1].Flags)
	})

	// Edge case tests for different flag combinations
	t.Run("No state changes when flags already in desired state", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)

		// Test case: account was already authorized, trying to authorize again
		tx := createTxWithTrustlineChanges(account, admin, asset, true, true, false, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges) // No changes needed as already in desired state
	})

	t.Run("Partial state changes when only some flags need changing", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)

		// Test case: account was not authorized, no maintain liabilities flag, authorizing
		tx := createTxWithTrustlineChanges(account, admin, asset, true, false, false, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, stateChanges, 1) // Only one state change needed

		// Only state change: Set AUTHORIZED_FLAG (maintain liabilities wasn't set, so no need to remove)
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonSet,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedFlagName}, stateChanges[0].Flags)
	})

	t.Run("Deauthorizing when already deauthorized but with different maintain liabilities state", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)

		// Test case: account was not authorized, no maintain liabilities flag, deauthorizing
		tx := createTxWithTrustlineChanges(account, admin, asset, false, false, false, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, stateChanges, 1) // Only one state change needed

		// Only state change: Set AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG (authorized flag wasn't set, so no need to remove)
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonSet,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedToMaintainLiabilitesFlagName}, stateChanges[0].Flags)
	})

	t.Run("Trustline change not found - should skip event", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)

		// Create transaction without trustline changes
		tx := createTxWithoutTrustlineChanges(account, admin, asset, true, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges) // Should skip the event due to missing trustline changes
	})

	t.Run("Trustline change missing previous state is ignored", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)

		// Create transaction where the trustline was just created (no Pre image)
		tx := createTxWithTrustlineCreation(account, admin, asset, true, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, stateChanges, 1) // Should create 1 state change for trustline authorization
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonSet,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedFlagName}, stateChanges[0].Flags)
	})

	// Error case tests
	t.Run("Invalid Operation Type - should return ErrInvalidOpType", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		tx := createSACInvocationTxV3(account, admin, asset, true)
		op, found := tx.GetOperation(0)
		require.True(t, found)

		// Change operation type to something other than InvokeHostFunction
		op.Body.Type = xdr.OperationTypePayment

		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.Error(t, err)
		require.ErrorIs(t, err, processors.ErrInvalidOpType)
		require.Nil(t, stateChanges)
	})

	t.Run("Invalid Contract Event - should be ignored", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		tx := createInvalidContractEventTx(account, admin, asset, true)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges)
	})

	t.Run("Trustline change for different asset is ignored", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)

		tx := createTxWithMismatchedTrustlineChanges(account, admin, asset, true, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges)
	})

	t.Run("Insufficient Topics - should be ignored", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		tx := createInsufficientTopicsTx(account, admin, asset, true)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges)
	})

	t.Run("Non-SAC Events - should be ignored", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		tx := createNonSACEventTx(account, admin, asset, true)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges)
	})

	// Contract Account Tests
	t.Run("Contract Account - set_authorized = true (was unauthorized)", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		contractAccount := generateContractAddress(asset) // Generate actual contract address
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)

		// Test case: contract was not authorized, now authorizing
		tx := createTxWithContractDataChanges(contractAccount, admin, asset, true, false, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, stateChanges, 1) // Should create 1 state change for contract authorization

		// State change: Set authorization for contract
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonSet,
			contractAccount,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Nil(t, stateChanges[0].Flags) // Contract accounts don't use flags
	})

	t.Run("Contract Account - set_authorized = false (was authorized)", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		contractAccount := generateContractAddress(asset)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)

		// Test case: contract was authorized, now deauthorizing
		tx := createTxWithContractDataChanges(contractAccount, admin, asset, false, true, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, stateChanges, 1) // Should create 1 state change for contract deauthorization

		// State change: Clear authorization for contract
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonClear,
			contractAccount,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Nil(t, stateChanges[0].Flags) // Contract accounts don't use flags
	})

	t.Run("Contract Account - No state changes when already in desired state", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		contractAccount := generateContractAddress(asset)

		// Test case: contract was already authorized, trying to authorize again
		tx := createTxWithContractDataChanges(contractAccount, admin, asset, true, true, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges) // No changes needed as already in desired state
	})

	t.Run("Contract Account - Contract data change not found", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		contractAccount := generateContractAddress(asset)

		// Create transaction without contract data changes
		tx := createTxWithoutContractDataChanges(contractAccount, admin, asset, true, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges) // Should skip the event due to missing contract data changes
	})

	t.Run("Contract data change for different contract is ignored", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		contractAccount := generateContractAddress(asset)

		tx := createTxWithMismatchedContractDataChanges(contractAccount, admin, asset, true, false, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges)
	})

	t.Run("Contract data change missing previous state is ignored", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		contractAccount := generateContractAddress(asset)

		// Create transaction where the contract balance was just created (no Pre image)
		tx := createTxWithContractDataCreation(contractAccount, admin, asset, true, 4)
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, stateChanges, 1) // Should create 1 state change for contract authorization
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonSet,
			contractAccount,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Nil(t, stateChanges[0].Flags) // Contract accounts don't use flags
	})

	// Contract Account Error Handling Tests
	t.Run("Contract Account - Invalid balance map (wrong entry count)", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		contractAccount := generateContractAddress(asset)

		// Create transaction with invalid balance map (wrong entry count)
		tx := createInvalidBalanceMapTx(contractAccount, admin, asset, true, "wrong_entry_count")
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges) // Should skip due to invalid balance map structure
	})

	t.Run("Contract Account - Invalid balance map (missing authorized key)", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		contractAccount := generateContractAddress(asset)

		// Create transaction with balance map missing 'authorized' key
		tx := createInvalidBalanceMapTx(contractAccount, admin, asset, true, "missing_authorized_key")
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges) // Should skip due to missing authorized key
	})

	t.Run("Contract Account - Invalid balance map (wrong authorized type)", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		contractAccount := generateContractAddress(asset)

		// Create transaction with wrong type for 'authorized' value
		tx := createInvalidBalanceMapTx(contractAccount, admin, asset, true, "wrong_authorized_type")
		op, found := tx.GetOperation(0)
		require.True(t, found)
		opWrapper := &operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        networkPassphrase,
			Transaction:    tx,
			LedgerSequence: 12345,
		}
		stateChanges, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Empty(t, stateChanges) // Should skip due to wrong data type for authorized
	})
}
