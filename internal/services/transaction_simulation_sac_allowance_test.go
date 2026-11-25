package services_test

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
)

// An approve call on a Stellar Asset Contract previews an allowance row for the
// owner, without any protocol classification in the database.
func TestTransactionSimulationService_sacApprove(t *testing.T) {
	nativeID, err := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(network.TestNetworkPassphrase)
	require.NoError(t, err)
	contractID := xdr.ContractId(nativeID)

	owner := keypair.MustRandom().Address()
	spender := keypair.MustRandom().Address()
	asset := xdr.ScString("native")
	approveEvent := sep41Event(contractID,
		[]xdr.ScVal{scSymbol("approve"), scAccountVal(owner), scAccountVal(spender), {Type: xdr.ScValTypeScvString, Str: &asset}},
		scVec(scI128(5_000_000), scU32(3_000_000)),
	)

	txXDR := invokeContractTxXDR(t, contractID)
	rpcMock := &services.RPCServiceMock{}
	rpcMock.On("SimulateTransaction", txXDR, entities.RPCResourceConfig{}).
		Return(entities.RPCSimulateTransactionResult{
			LatestLedger:   2900148,
			MinResourceFee: "100",
			Events:         []string{diagnosticB64(t, approveEvent)},
		}, nil).Once()

	svc, err := services.NewTransactionSimulationService(rpcMock, nil, network.TestNetworkPassphrase, nil)
	require.NoError(t, err)

	result, err := svc.SimulateStateChanges(context.Background(), txXDR)
	require.NoError(t, err)

	// The fee debit is also on native XLM, so pick the allowance rows out.
	var allowances []types.StateChange
	for _, sc := range changesForToken(result.StateChanges, contractIDStrkey(t, contractID)) {
		if sc.StateChangeCategory == types.StateChangeCategoryAllowance {
			allowances = append(allowances, sc)
		}
	}
	require.Len(t, allowances, 1)
	allowance := allowances[0]
	assert.Equal(t, types.StateChangeReasonUpdate, allowance.StateChangeReason)
	assert.Equal(t, owner, string(allowance.AccountID))
	assert.Equal(t, spender, allowance.SpenderAccountID.String())
	assert.Equal(t, "5000000", allowance.Amount.String)
	assert.Equal(t, uint32(3_000_000), allowance.KeyValue["live_until_ledger"])
	assert.Equal(t, int32(1), toid.Parse(allowance.OperationID).OperationOrder, "row belongs to the first operation")

	rpcMock.AssertExpectations(t)
}
