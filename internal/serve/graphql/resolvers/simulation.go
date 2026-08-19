package resolvers

import (
	"fmt"
	"math"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

// convertToSimulatedStateChanges resolves the concrete simulated GraphQL type for
// each state change produced by the simulation pipeline. Unlike the history types
// (convertStateChangeTypes), simulated types are plain structs built eagerly: the
// data never touches the DB, so there are no lazy column-backed resolvers.
func (r *Resolver) convertToSimulatedStateChanges(stateChanges []types.StateChange) ([]graphql1.BaseSimulatedStateChange, error) {
	converted := make([]graphql1.BaseSimulatedStateChange, len(stateChanges))
	for i, stateChange := range stateChanges {
		c, err := r.convertToSimulatedStateChange(stateChange)
		if err != nil {
			return nil, err
		}
		converted[i] = c
	}
	return converted, nil
}

// convertToSimulatedStateChange dispatches on (category, reason), mirroring
// convertStateChangeTypes. A change matching no arm means the simulation pipeline
// emitted a variant the simulated schema does not expose yet, and surfaces as an
// error rather than a silently dropped row.
func (r *Resolver) convertToSimulatedStateChange(sc types.StateChange) (graphql1.BaseSimulatedStateChange, error) {
	accountAddress := string(sc.AccountID)

	switch sc.StateChangeCategory {
	case types.StateChangeCategoryBalance:
		switch sc.StateChangeReason {
		case types.StateChangeReasonDebit, types.StateChangeReasonCredit, types.StateChangeReasonMint, types.StateChangeReasonBurn:
			tokenID, err := r.resolveRequiredAddress(sc.TokenID, "tokenId")
			if err != nil {
				return nil, err
			}
			amount, err := r.resolveRequiredString(sc.Amount, "amount")
			if err != nil {
				return nil, err
			}
			return graphql1.SimulatedBalanceChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				TokenID:        tokenID,
				Amount:         amount,
				ToMuxedID:      r.resolveNullableString(sc.ToMuxedID),
			}, nil
		default: // invalid reason for BALANCE; falls through to the error below
		}
	case types.StateChangeCategoryAccount:
		switch sc.StateChangeReason {
		case types.StateChangeReasonCreate:
			creatorAddress, err := r.resolveRequiredAddress(sc.CreatorAccountID, "creatorAddress")
			if err != nil {
				return nil, err
			}
			return graphql1.SimulatedAccountCreatedChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				CreatorAddress: creatorAddress,
			}, nil
		default: // invalid reason for ACCOUNT; falls through to the error below
		}
	case types.StateChangeCategoryAllowance:
		switch sc.StateChangeReason {
		case types.StateChangeReasonUpdate:
			tokenID, err := r.resolveRequiredAddress(sc.TokenID, "tokenId")
			if err != nil {
				return nil, err
			}
			spender, err := r.resolveRequiredAddress(sc.SpenderAccountID, "spender")
			if err != nil {
				return nil, err
			}
			amount, err := r.resolveRequiredString(sc.Amount, "amount")
			if err != nil {
				return nil, err
			}
			expirationLedger, ok := keyValueUint32(sc.KeyValue, "live_until_ledger")
			if !ok {
				return nil, fmt.Errorf("state change is missing required expirationLedger")
			}
			return graphql1.SimulatedAllowanceChange{
				Category:         sc.StateChangeCategory,
				Reason:           sc.StateChangeReason,
				AccountAddress:   accountAddress,
				TokenID:          tokenID,
				Spender:          spender,
				Amount:           amount,
				ExpirationLedger: expirationLedger,
			}, nil
		default: // invalid reason for ALLOWANCE; falls through to the error below
		}
	case types.StateChangeCategoryBalanceAuthorization:
		switch sc.StateChangeReason {
		case types.StateChangeReasonSet, types.StateChangeReasonClear:
			var flags []types.TrustlineFlag
			if sc.Flags.Valid {
				flags = types.DecodeTrustlineFlags(sc.Flags.Int16)
			}
			return graphql1.SimulatedBalanceAuthorizationChange{
				Category:        sc.StateChangeCategory,
				Reason:          sc.StateChangeReason,
				AccountAddress:  accountAddress,
				TokenID:         r.resolveNullableAddress(sc.TokenID),
				LiquidityPoolID: r.resolveNullableString(sc.LiquidityPoolID),
				Flags:           flags,
			}, nil
		default: // invalid reason for BALANCE_AUTHORIZATION; falls through to the error below
		}
	case types.StateChangeCategorySigner, types.StateChangeCategorySignatureThreshold,
		types.StateChangeCategoryDataEntry, types.StateChangeCategoryHomeDomain,
		types.StateChangeCategoryFlags, types.StateChangeCategoryTrustline:
		// Classic-source variants: the simulated schema doesn't expose them yet.
		// They land with the classic derivation source; falls through to the error below.
	}
	return nil, fmt.Errorf("state change has no simulated GraphQL type for (category=%s, reason=%s)",
		sc.StateChangeCategory, sc.StateChangeReason)
}

// keyValueUint32 reads a uint32 from a KeyValue JSONB payload, where JSON
// unmarshal exposes numbers as float64.
func keyValueUint32(kv types.NullableJSONB, key string) (uint32, bool) {
	raw, ok := kv[key]
	if !ok {
		return 0, false
	}
	f, ok := raw.(float64)
	if !ok || f < 0 || f > math.MaxUint32 {
		return 0, false
	}
	return uint32(f), true
}
