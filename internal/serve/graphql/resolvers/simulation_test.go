package resolvers

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

const (
	testSimAccount = "GBF3XFXGBGNQDN3HOSZ7NVRF6TJ2JOD5U6ELIWJOOEI6T5WKMQT2YSXQ"
	testSimToken   = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	testSimSpender = "GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"
)

func validAddress(addr string) types.NullAddressBytea {
	return types.NullAddressBytea{AddressBytea: types.AddressBytea(addr), Valid: true}
}

func TestConvertToSimulatedStateChange(t *testing.T) {
	r := &Resolver{}

	t.Run("🟢 balance change", func(t *testing.T) {
		converted, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryBalance,
			StateChangeReason:   types.StateChangeReasonDebit,
			AccountID:           types.AddressBytea(testSimAccount),
			TokenID:             validAddress(testSimToken),
			Amount:              sql.NullString{String: "10000000", Valid: true},
		})
		require.NoError(t, err)
		balance, ok := converted.(graphql1.SimulatedBalanceChange)
		require.True(t, ok, "expected SimulatedBalanceChange, got %T", converted)
		assert.Equal(t, testSimAccount, balance.AccountAddress)
		assert.Equal(t, testSimToken, balance.TokenID)
		assert.Equal(t, "10000000", balance.Amount)
		assert.Nil(t, balance.ToMuxedID)
	})

	t.Run("🟢 account created change", func(t *testing.T) {
		converted, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryAccount,
			StateChangeReason:   types.StateChangeReasonCreate,
			AccountID:           types.AddressBytea(testSimToken), // a deployed contract
			CreatorAccountID:    validAddress(testSimAccount),
		})
		require.NoError(t, err)
		created, ok := converted.(graphql1.SimulatedAccountCreatedChange)
		require.True(t, ok, "expected SimulatedAccountCreatedChange, got %T", converted)
		assert.Equal(t, testSimToken, created.AccountAddress)
		assert.Equal(t, testSimAccount, created.CreatorAddress)
	})

	// The allowance expiration arrives in two shapes: a raw uint32 when the state
	// change was built in memory (the simulation path: sep41.Processor stages
	// live_until_ledger as uint32) and a float64 after a JSONB round-trip. The
	// converter must accept both; the uint32 case is the one every simulated
	// SEP-41 approve actually takes.
	for name, liveUntil := range map[string]any{
		"uint32 (in-memory / simulation path)": uint32(3_000_000),
		"float64 (JSONB round-trip)":           float64(3_000_000),
	} {
		t.Run("🟢 allowance change with live_until_ledger as "+name, func(t *testing.T) {
			converted, err := r.convertToSimulatedStateChange(types.StateChange{
				StateChangeCategory: types.StateChangeCategoryAllowance,
				StateChangeReason:   types.StateChangeReasonUpdate,
				AccountID:           types.AddressBytea(testSimAccount),
				TokenID:             validAddress(testSimToken),
				SpenderAccountID:    validAddress(testSimSpender),
				Amount:              sql.NullString{String: "5000000", Valid: true},
				KeyValue:            types.NullableJSONB{"live_until_ledger": liveUntil},
			})
			require.NoError(t, err)
			allowance, ok := converted.(graphql1.SimulatedAllowanceChange)
			require.True(t, ok, "expected SimulatedAllowanceChange, got %T", converted)
			assert.Equal(t, testSimAccount, allowance.AccountAddress)
			assert.Equal(t, testSimToken, allowance.TokenID)
			assert.Equal(t, testSimSpender, allowance.Spender)
			assert.Equal(t, "5000000", allowance.Amount)
			assert.Equal(t, uint32(3_000_000), allowance.ExpirationLedger)
		})
	}

	t.Run("🔴 allowance change without live_until_ledger errors", func(t *testing.T) {
		_, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryAllowance,
			StateChangeReason:   types.StateChangeReasonUpdate,
			AccountID:           types.AddressBytea(testSimAccount),
			TokenID:             validAddress(testSimToken),
			SpenderAccountID:    validAddress(testSimSpender),
			Amount:              sql.NullString{String: "5000000", Valid: true},
			KeyValue:            types.NullableJSONB{},
		})
		require.ErrorContains(t, err, "expirationLedger")
	})

	t.Run("🟢 balance authorization change with trustline flags", func(t *testing.T) {
		converted, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryBalanceAuthorization,
			StateChangeReason:   types.StateChangeReasonSet,
			AccountID:           types.AddressBytea(testSimAccount),
			TokenID:             validAddress(testSimToken),
			Flags:               sql.NullInt16{Int16: 1, Valid: true},
		})
		require.NoError(t, err)
		auth, ok := converted.(graphql1.SimulatedBalanceAuthorizationChange)
		require.True(t, ok, "expected SimulatedBalanceAuthorizationChange, got %T", converted)
		require.NotNil(t, auth.TokenID)
		assert.Equal(t, testSimToken, *auth.TokenID)
		assert.Equal(t, types.DecodeTrustlineFlags(1), auth.Flags)
	})

	t.Run("🟢 balance authorization change without flags (SAC contract holder)", func(t *testing.T) {
		converted, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryBalanceAuthorization,
			StateChangeReason:   types.StateChangeReasonClear,
			AccountID:           types.AddressBytea(testSimToken),
			TokenID:             validAddress(testSimToken),
		})
		require.NoError(t, err)
		auth, ok := converted.(graphql1.SimulatedBalanceAuthorizationChange)
		require.True(t, ok)
		assert.Nil(t, auth.Flags, "contract-holder authorization has no trustline flags")
	})

	t.Run("🔴 missing required field errors instead of emitting a partial row", func(t *testing.T) {
		_, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryBalance,
			StateChangeReason:   types.StateChangeReasonCredit,
			AccountID:           types.AddressBytea(testSimAccount),
			TokenID:             validAddress(testSimToken),
			// Amount missing
		})
		require.ErrorContains(t, err, "amount")
	})

	t.Run("🔴 variant not exposed in the simulated schema errors", func(t *testing.T) {
		_, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategorySigner,
			StateChangeReason:   types.StateChangeReasonAdd,
			AccountID:           types.AddressBytea(testSimAccount),
		})
		require.ErrorContains(t, err, "no simulated GraphQL type")
	})
}
