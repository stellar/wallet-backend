package processors

import (
	"context"
	"math/big"
	"testing"

	"github.com/stellar/go/keypair"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

var (
	someContractID1 = xdr.ContractId([32]byte{1, 2, 3, 4})
	someContractID2 = xdr.ContractId([32]byte{5, 6, 7, 8})
)

func TestEventsProcessor_ProcessOperation(t *testing.T) {
	processor := NewEventsProcessor(networkPassphrase)

	t.Run("V3 Format - Set Allowance - generate a state change for the source account", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		amount := big.NewInt(100)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		tx := createSep41InvocationTxV3(someContractID1, someContractID2, admin, asset, amount)
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
		require.Len(t, stateChanges, 1)
		assertContractEvent(t, stateChanges[0], types.StateChangeCategoryAllowance, types.StateChangeReasonSet,
			strkey.MustEncode(strkey.VersionByteContract, someContractID1[:]), "100",
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
	})

	t.Run("V4 Format - Set Allowance with direct i128 amount", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		amount := big.NewInt(250)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		tx := createSep41InvocationTxV4(someContractID1, someContractID2, admin, asset, amount, false)
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
		require.Len(t, stateChanges, 1)
		assertContractEvent(t, stateChanges[0], types.StateChangeCategoryAllowance, types.StateChangeReasonSet,
			strkey.MustEncode(strkey.VersionByteContract, someContractID1[:]), "250",
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
	})

	t.Run("V4 Format - Set Allowance with map data format", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		amount := big.NewInt(500)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		tx := createSep41InvocationTxV4(someContractID1, someContractID2, admin, asset, amount, true)
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
		require.Len(t, stateChanges, 1)
		assertContractEvent(t, stateChanges[0], types.StateChangeCategoryAllowance, types.StateChangeReasonSet,
			strkey.MustEncode(strkey.VersionByteContract, someContractID1[:]), "500",
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
	})

	t.Run("Multiple Approve Events - generate multiple state changes", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		amount := big.NewInt(75)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		tx := createMultipleApproveEventsTx(someContractID1, someContractID2, admin, asset, amount)
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
		require.Len(t, stateChanges, 2)

		// First approve event
		assertContractEvent(t, stateChanges[0], types.StateChangeCategoryAllowance, types.StateChangeReasonSet,
			strkey.MustEncode(strkey.VersionByteContract, someContractID1[:]), "75",
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))

		// Second approve event
		assertContractEvent(t, stateChanges[1], types.StateChangeCategoryAllowance, types.StateChangeReasonSet,
			strkey.MustEncode(strkey.VersionByteContract, someContractID2[:]), "75",
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
	})

	// Error case tests
	t.Run("Invalid Operation Type - should return ErrInvalidOpType", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		amount := big.NewInt(100)
		tx := createSep41InvocationTxV3(someContractID1, someContractID2, admin, asset, amount)
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
		require.ErrorIs(t, err, ErrInvalidOpType)
		require.Nil(t, stateChanges)
	})

	t.Run("Invalid Contract Event - should be ignored", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		amount := big.NewInt(100)
		tx := createInvalidContractEventTx(someContractID1, someContractID2, admin, asset, amount)
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
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		amount := big.NewInt(100)
		tx := createInsufficientTopicsTx(someContractID1, admin, asset, amount)
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

	t.Run("Non-Approve Events - should be ignored", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		amount := big.NewInt(100)
		tx := createNonApproveEventTx(someContractID1, someContractID2, admin, asset, amount)
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
