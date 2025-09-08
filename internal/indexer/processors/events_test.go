package processors

import (
	"context"
	"math/big"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stretchr/testify/require"
)

var (
	someContractId1 = xdr.ContractId([32]byte{1, 2, 3, 4})
	someContractId2 = xdr.ContractId([32]byte{5, 6, 7, 8})
)

func TestEventsProcessor_ProcessOperation(t *testing.T) {
	processor := NewEventsProcessor(networkPassphrase)
	t.Run("Set Allowance - generate a state change for the source account for which the allowance was set", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		amount := big.NewInt(100)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		tx := createSep41InvocationTxV3(someContractId1, someContractId2, admin, asset, amount)
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
			strkey.MustEncode(strkey.VersionByteContract, someContractId1[:]), "100", 
			strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
	})
}
