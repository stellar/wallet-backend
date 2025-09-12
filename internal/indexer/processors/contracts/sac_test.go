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

	t.Run("V3 Format - set_authorized = true", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		tx := createSACInvocationTxV3(account, admin, asset, true)
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
		assertContractEvent(t, stateChanges[1], types.StateChangeReasonRemove,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedToMaintainLiabilitesFlagName}, stateChanges[1].Flags)
	})

	t.Run("V4 Format - set_authorized = true", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		tx := createSACInvocationTxV4(account, admin, asset, true)
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
		assertContractEvent(t, stateChanges[1], types.StateChangeReasonRemove,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedToMaintainLiabilitesFlagName}, stateChanges[1].Flags)
	})

	t.Run("V4 Format - set_authorized = false", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		account := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TESTASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		tx := createSACInvocationTxV4(account, admin, asset, false)
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
		assertContractEvent(t, stateChanges[0], types.StateChangeReasonRemove,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedFlagName}, stateChanges[0].Flags)

		// Second state change: Set AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG
		assertContractEvent(t, stateChanges[1], types.StateChangeReasonSet,
			account,
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		require.Equal(t, types.NullableJSON{AuthorizedToMaintainLiabilitesFlagName}, stateChanges[1].Flags)
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
}
