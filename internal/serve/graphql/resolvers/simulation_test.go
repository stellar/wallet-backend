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

	t.Run("🔴 allowance change with wrong-typed live_until_ledger reports the type, not a missing value", func(t *testing.T) {
		_, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryAllowance,
			StateChangeReason:   types.StateChangeReasonUpdate,
			AccountID:           types.AddressBytea(testSimAccount),
			TokenID:             validAddress(testSimToken),
			SpenderAccountID:    validAddress(testSimSpender),
			Amount:              sql.NullString{String: "5000000", Valid: true},
			KeyValue:            types.NullableJSONB{"live_until_ledger": "3000000"},
		})
		require.ErrorContains(t, err, "unexpected type string")
	})

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
		// accountMerge is not derivable yet, so (ACCOUNT, MERGE) has no
		// simulated type until its classic handler lands.
		_, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryAccount,
			StateChangeReason:   types.StateChangeReasonMerge,
			AccountID:           types.AddressBytea(testSimAccount),
		})
		require.ErrorContains(t, err, "no simulated GraphQL type")
	})

	t.Run("🟢 signer variants", func(t *testing.T) {
		base := types.StateChange{
			StateChangeCategory: types.StateChangeCategorySigner,
			AccountID:           types.AddressBytea(testSimAccount),
			SignerAccountID:     validAddress(testSimSpender),
			SignerWeightOld:     sql.NullInt16{Int16: 3, Valid: true},
			SignerWeightNew:     sql.NullInt16{Int16: 5, Valid: true},
		}

		add := base
		add.StateChangeReason = types.StateChangeReasonAdd
		converted, err := r.convertToSimulatedStateChange(add)
		require.NoError(t, err)
		added, ok := converted.(graphql1.SimulatedSignerAddedChange)
		require.True(t, ok, "expected SimulatedSignerAddedChange, got %T", converted)
		assert.Equal(t, testSimSpender, added.SignerAddress)
		assert.Equal(t, int32(5), added.NewWeight)

		update := base
		update.StateChangeReason = types.StateChangeReasonUpdate
		converted, err = r.convertToSimulatedStateChange(update)
		require.NoError(t, err)
		updated, ok := converted.(graphql1.SimulatedSignerUpdatedChange)
		require.True(t, ok, "expected SimulatedSignerUpdatedChange, got %T", converted)
		assert.Equal(t, int32(3), updated.OldWeight)
		assert.Equal(t, int32(5), updated.NewWeight)

		remove := base
		remove.StateChangeReason = types.StateChangeReasonRemove
		converted, err = r.convertToSimulatedStateChange(remove)
		require.NoError(t, err)
		removed, ok := converted.(graphql1.SimulatedSignerRemovedChange)
		require.True(t, ok, "expected SimulatedSignerRemovedChange, got %T", converted)
		assert.Equal(t, int32(3), removed.OldWeight)
	})

	t.Run("🟢 threshold change", func(t *testing.T) {
		converted, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategorySignatureThreshold,
			StateChangeReason:   types.StateChangeReasonUpdate,
			AccountID:           types.AddressBytea(testSimAccount),
			Threshold:           sql.NullString{String: string(types.ThresholdLevelMedium), Valid: true},
			ThresholdOld:        sql.NullInt16{Int16: 1, Valid: true},
			ThresholdNew:        sql.NullInt16{Int16: 2, Valid: true},
		})
		require.NoError(t, err)
		threshold, ok := converted.(graphql1.SimulatedThresholdChange)
		require.True(t, ok, "expected SimulatedThresholdChange, got %T", converted)
		assert.Equal(t, types.ThresholdLevelMedium, threshold.Threshold)
		assert.Equal(t, int32(1), threshold.OldThreshold)
		assert.Equal(t, int32(2), threshold.NewThreshold)
	})

	t.Run("🟢 account flags change", func(t *testing.T) {
		converted, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryFlags,
			StateChangeReason:   types.StateChangeReasonSet,
			AccountID:           types.AddressBytea(testSimAccount),
			Flags:               sql.NullInt16{Int16: 1, Valid: true},
		})
		require.NoError(t, err)
		flags, ok := converted.(graphql1.SimulatedAccountFlagsChange)
		require.True(t, ok, "expected SimulatedAccountFlagsChange, got %T", converted)
		assert.Equal(t, types.DecodeAccountFlags(1), flags.Flags)
	})

	t.Run("🔴 flags change without flags value errors", func(t *testing.T) {
		_, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryFlags,
			StateChangeReason:   types.StateChangeReasonSet,
			AccountID:           types.AddressBytea(testSimAccount),
		})
		require.ErrorContains(t, err, "flags")
	})

	t.Run("🟢 home domain variants", func(t *testing.T) {
		set, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryHomeDomain,
			StateChangeReason:   types.StateChangeReasonSet,
			AccountID:           types.AddressBytea(testSimAccount),
			KeyValue:            types.NullableJSONB{"new": "example.com"},
		})
		require.NoError(t, err)
		setChange, ok := set.(graphql1.SimulatedHomeDomainSetChange)
		require.True(t, ok, "expected SimulatedHomeDomainSetChange, got %T", set)
		assert.Equal(t, "example.com", setChange.HomeDomain)

		updated, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryHomeDomain,
			StateChangeReason:   types.StateChangeReasonUpdate,
			AccountID:           types.AddressBytea(testSimAccount),
			KeyValue:            types.NullableJSONB{"old": "a.com", "new": "b.com"},
		})
		require.NoError(t, err)
		updatedChange, ok := updated.(graphql1.SimulatedHomeDomainUpdatedChange)
		require.True(t, ok, "expected SimulatedHomeDomainUpdatedChange, got %T", updated)
		assert.Equal(t, "a.com", updatedChange.OldHomeDomain)
		assert.Equal(t, "b.com", updatedChange.NewHomeDomain)

		cleared, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryHomeDomain,
			StateChangeReason:   types.StateChangeReasonClear,
			AccountID:           types.AddressBytea(testSimAccount),
			KeyValue:            types.NullableJSONB{"old": "a.com"},
		})
		require.NoError(t, err)
		clearedChange, ok := cleared.(graphql1.SimulatedHomeDomainClearedChange)
		require.True(t, ok, "expected SimulatedHomeDomainClearedChange, got %T", cleared)
		assert.Equal(t, "a.com", clearedChange.OldHomeDomain)
	})

	t.Run("🟢 data entry variants", func(t *testing.T) {
		added, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryDataEntry,
			StateChangeReason:   types.StateChangeReasonAdd,
			AccountID:           types.AddressBytea(testSimAccount),
			DataEntryName:       sql.NullString{String: "config", Valid: true},
			KeyValue:            types.NullableJSONB{"new": "djE="},
		})
		require.NoError(t, err)
		addedChange, ok := added.(graphql1.SimulatedDataEntryAddedChange)
		require.True(t, ok, "expected SimulatedDataEntryAddedChange, got %T", added)
		assert.Equal(t, "config", addedChange.Name)
		assert.Equal(t, "djE=", addedChange.Value)

		removed, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryDataEntry,
			StateChangeReason:   types.StateChangeReasonRemove,
			AccountID:           types.AddressBytea(testSimAccount),
			DataEntryName:       sql.NullString{String: "config", Valid: true},
			KeyValue:            types.NullableJSONB{"old": "djE="},
		})
		require.NoError(t, err)
		removedChange, ok := removed.(graphql1.SimulatedDataEntryRemovedChange)
		require.True(t, ok, "expected SimulatedDataEntryRemovedChange, got %T", removed)
		assert.Equal(t, "djE=", removedChange.OldValue)
	})

	t.Run("🟢 trustline variants", func(t *testing.T) {
		added, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryTrustline,
			StateChangeReason:   types.StateChangeReasonAdd,
			AccountID:           types.AddressBytea(testSimAccount),
			TokenID:             validAddress(testSimToken),
			TrustlineLimitNew:   sql.NullString{String: "10000000000", Valid: true},
		})
		require.NoError(t, err)
		addedChange, ok := added.(graphql1.SimulatedTrustlineAddedChange)
		require.True(t, ok, "expected SimulatedTrustlineAddedChange, got %T", added)
		require.NotNil(t, addedChange.TokenID)
		assert.Equal(t, testSimToken, *addedChange.TokenID)
		assert.Equal(t, "10000000000", addedChange.Limit)
		assert.Nil(t, addedChange.LiquidityPoolID)

		updated, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryTrustline,
			StateChangeReason:   types.StateChangeReasonUpdate,
			AccountID:           types.AddressBytea(testSimAccount),
			TokenID:             validAddress(testSimToken),
			TrustlineLimitOld:   sql.NullString{String: "5", Valid: true},
			TrustlineLimitNew:   sql.NullString{String: "9", Valid: true},
		})
		require.NoError(t, err)
		updatedChange, ok := updated.(graphql1.SimulatedTrustlineUpdatedChange)
		require.True(t, ok, "expected SimulatedTrustlineUpdatedChange, got %T", updated)
		assert.Equal(t, "5", updatedChange.OldLimit)
		assert.Equal(t, "9", updatedChange.NewLimit)

		removed, err := r.convertToSimulatedStateChange(types.StateChange{
			StateChangeCategory: types.StateChangeCategoryTrustline,
			StateChangeReason:   types.StateChangeReasonRemove,
			AccountID:           types.AddressBytea(testSimAccount),
			TokenID:             validAddress(testSimToken),
		})
		require.NoError(t, err)
		removedChange, ok := removed.(graphql1.SimulatedTrustlineRemovedChange)
		require.True(t, ok, "expected SimulatedTrustlineRemovedChange, got %T", removed)
		require.NotNil(t, removedChange.TokenID)
		assert.Equal(t, testSimToken, *removedChange.TokenID)
	})
}
