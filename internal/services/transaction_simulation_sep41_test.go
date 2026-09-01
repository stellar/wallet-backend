package services_test

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	_ "github.com/stellar/wallet-backend/internal/services/sep41" // registers the SEP-41 processor via init()
)

// TestTransactionSimulationService_customSEP41Token drives the SEP-41 path of
// simulateStateChanges end to end: a simulated invocation of a custom
// (non-SAC) token that WB has classified as SEP-41 must preview the same
// balance and allowance state changes history would show. An unclassified
// contract must be silently skipped, mirroring live ingestion.
func TestTransactionSimulationService_customSEP41Token(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	contractID := randomContractID(t)
	classifyAsSEP41(t, ctx, pool, models, contractID)

	holder := keypair.MustRandom().Address()
	receiver := keypair.MustRandom().Address()
	spender := keypair.MustRandom().Address()

	transferEvent := sep41Event(contractID,
		[]xdr.ScVal{scSymbol("transfer"), scAccountVal(holder), scAccountVal(receiver)},
		scI128(10_000_000),
	)
	approveEvent := sep41Event(contractID,
		[]xdr.ScVal{scSymbol("approve"), scAccountVal(holder), scAccountVal(spender)},
		scVec(scI128(5_000_000), scU32(3_000_000)),
	)

	txXDR := invokeContractTxXDR(t, contractID)
	rpcMock := &services.RPCServiceMock{}
	rpcMock.On("SimulateTransaction", txXDR, entities.RPCResourceConfig{}).
		Return(entities.RPCSimulateTransactionResult{
			LatestLedger:   2900148,
			MinResourceFee: "100",
			Events:         []string{diagnosticB64(t, transferEvent), diagnosticB64(t, approveEvent)},
		}, nil).Once()

	svc, err := services.NewTransactionSimulationService(rpcMock, models, network.TestNetworkPassphrase)
	require.NoError(t, err)

	result, err := svc.SimulateStateChanges(ctx, txXDR)
	require.NoError(t, err)

	tokenStrkey := contractIDStrkey(t, contractID)
	tokenChanges := changesForToken(result.StateChanges, tokenStrkey)
	require.Len(t, tokenChanges, 3, "expected debit + credit + allowance for the custom token")

	byReason := map[types.StateChangeReason]types.StateChange{}
	for _, sc := range tokenChanges {
		byReason[sc.StateChangeReason] = sc
	}

	debit, ok := byReason[types.StateChangeReasonDebit]
	require.True(t, ok, "expected a DEBIT for the holder")
	assert.Equal(t, holder, string(debit.AccountID))
	assert.Equal(t, "10000000", debit.Amount.String)
	assert.Equal(t, types.StateChangeCategoryBalance, debit.StateChangeCategory)

	credit, ok := byReason[types.StateChangeReasonCredit]
	require.True(t, ok, "expected a CREDIT for the receiver")
	assert.Equal(t, receiver, string(credit.AccountID))
	assert.Equal(t, "10000000", credit.Amount.String)

	allowance, ok := byReason[types.StateChangeReasonUpdate]
	require.True(t, ok, "expected an ALLOWANCE update for the holder")
	assert.Equal(t, types.StateChangeCategoryAllowance, allowance.StateChangeCategory)
	assert.Equal(t, holder, string(allowance.AccountID))
	assert.Equal(t, spender, allowance.SpenderAccountID.String())
	assert.Equal(t, "5000000", allowance.Amount.String)
	// In-memory staged rows carry the value as uint32; the float64 shape only
	// appears after a JSONB round-trip, which simulated rows never take.
	assert.Equal(t, uint32(3_000_000), allowance.KeyValue["live_until_ledger"])

	rpcMock.AssertExpectations(t)

	t.Run("unclassified contract is silently skipped", func(t *testing.T) {
		unknownID := randomContractID(t)
		unknownEvent := sep41Event(unknownID,
			[]xdr.ScVal{scSymbol("transfer"), scAccountVal(holder), scAccountVal(receiver)},
			scI128(1),
		)
		unknownTxXDR := invokeContractTxXDR(t, unknownID)
		rpcMock := &services.RPCServiceMock{}
		rpcMock.On("SimulateTransaction", unknownTxXDR, entities.RPCResourceConfig{}).
			Return(entities.RPCSimulateTransactionResult{
				LatestLedger:   2900149,
				MinResourceFee: "100",
				Events:         []string{diagnosticB64(t, unknownEvent)},
			}, nil).Once()

		svc, err := services.NewTransactionSimulationService(rpcMock, models, network.TestNetworkPassphrase)
		require.NoError(t, err)

		result, err := svc.SimulateStateChanges(ctx, unknownTxXDR)
		require.NoError(t, err)
		assert.Empty(t, changesForToken(result.StateChanges, contractIDStrkey(t, unknownID)),
			"an unclassified contract must produce no token state changes")
		rpcMock.AssertExpectations(t)
	})
}

// classifyAsSEP41 registers the contract in protocol_wasms + protocol_contracts,
// the same rows the SEP-41 validator commits at classification time.
func classifyAsSEP41(t *testing.T, ctx context.Context, pool *pgxpool.Pool, models *data.Models, contractID xdr.ContractId) {
	t.Helper()
	wasmHash := make([]byte, 32)
	_, err := rand.Read(wasmHash)
	require.NoError(t, err)

	dbTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = dbTx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	protocolID := "SEP41"
	_, err = dbTx.Exec(ctx, `INSERT INTO protocols (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, protocolID)
	require.NoError(t, err)
	require.NoError(t, models.ProtocolWasms.BatchInsert(ctx, dbTx, []data.ProtocolWasms{{
		WasmHash:   types.HashBytea(hexString(wasmHash)),
		ProtocolID: &protocolID,
	}}))
	require.NoError(t, models.ProtocolContracts.BatchInsert(ctx, dbTx, []data.ProtocolContracts{{
		ContractID: types.HashBytea(hexString(contractID[:])),
		WasmHash:   types.HashBytea(hexString(wasmHash)),
	}}))
	require.NoError(t, dbTx.Commit(ctx))
}

func changesForToken(stateChanges []types.StateChange, tokenStrkey string) []types.StateChange {
	var out []types.StateChange
	for _, sc := range stateChanges {
		if sc.TokenID.String() == tokenStrkey {
			out = append(out, sc)
		}
	}
	return out
}

func randomContractID(t *testing.T) xdr.ContractId {
	t.Helper()
	var id xdr.ContractId
	_, err := rand.Read(id[:])
	require.NoError(t, err)
	return id
}

func contractIDStrkey(t *testing.T, contractID xdr.ContractId) string {
	t.Helper()
	addr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID}
	s, err := addr.String()
	require.NoError(t, err)
	return s
}

// sep41Event builds a contract event in the shape internal/services/sep41
// parses: topic[0] the function symbol, address topics, and the amount payload.
func sep41Event(contractID xdr.ContractId, topics []xdr.ScVal, payload xdr.ScVal) xdr.ContractEvent {
	return xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &contractID,
		Body: xdr.ContractEventBody{
			V:  0,
			V0: &xdr.ContractEventV0{Topics: topics, Data: payload},
		},
	}
}

func diagnosticB64(t *testing.T, event xdr.ContractEvent) string {
	t.Helper()
	b64, err := xdr.MarshalBase64(xdr.DiagnosticEvent{InSuccessfulContractCall: true, Event: event})
	require.NoError(t, err)
	return b64
}

func invokeContractTxXDR(t *testing.T, contractID xdr.ContractId) string {
	t.Helper()
	src := txnbuild.SimpleAccount{AccountID: keypair.MustRandom().Address(), Sequence: 1}
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &src,
		Operations: []txnbuild.Operation{&txnbuild.InvokeHostFunction{
			HostFunction: xdr.HostFunction{
				Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
				InvokeContract: &xdr.InvokeContractArgs{
					ContractAddress: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID},
					FunctionName:    "transfer",
				},
			},
		}},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		IncrementSequenceNum: true,
	})
	require.NoError(t, err)
	b64, err := tx.Base64()
	require.NoError(t, err)
	return b64
}

func scSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func scAccountVal(address string) xdr.ScVal {
	accountID := xdr.MustAddress(address)
	addr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &accountID}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}
}

func scI128(amount int64) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Hi: 0, Lo: xdr.Uint64(amount)}}
}

func scU32(v uint32) xdr.ScVal {
	u := xdr.Uint32(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
}

func scVec(vals ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(vals)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}

func hexString(b []byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, hexdigits[c>>4], hexdigits[c&0x0f])
	}
	return string(out)
}
