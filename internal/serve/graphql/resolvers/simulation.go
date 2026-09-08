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
			expirationLedger, err := keyValueUint32(sc.KeyValue, "live_until_ledger")
			if err != nil {
				return nil, fmt.Errorf("resolving expirationLedger: %w", err)
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
	case types.StateChangeCategorySigner:
		signerAddress, err := r.resolveRequiredAddress(sc.SignerAccountID, "signerAddress")
		if err != nil {
			return nil, err
		}
		switch sc.StateChangeReason {
		case types.StateChangeReasonAdd:
			newWeight, err := r.resolveRequiredInt16(sc.SignerWeightNew, "newWeight")
			if err != nil {
				return nil, err
			}
			return graphql1.SimulatedSignerAddedChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				SignerAddress:  signerAddress,
				NewWeight:      newWeight,
			}, nil
		case types.StateChangeReasonUpdate:
			oldWeight, err := r.resolveRequiredInt16(sc.SignerWeightOld, "oldWeight")
			if err != nil {
				return nil, err
			}
			newWeight, err := r.resolveRequiredInt16(sc.SignerWeightNew, "newWeight")
			if err != nil {
				return nil, err
			}
			return graphql1.SimulatedSignerUpdatedChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				SignerAddress:  signerAddress,
				OldWeight:      oldWeight,
				NewWeight:      newWeight,
			}, nil
		case types.StateChangeReasonRemove:
			oldWeight, err := r.resolveRequiredInt16(sc.SignerWeightOld, "oldWeight")
			if err != nil {
				return nil, err
			}
			return graphql1.SimulatedSignerRemovedChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				SignerAddress:  signerAddress,
				OldWeight:      oldWeight,
			}, nil
		default: // invalid reason for SIGNER; falls through to the error below
		}
	case types.StateChangeCategorySignatureThreshold:
		switch sc.StateChangeReason {
		case types.StateChangeReasonUpdate:
			level, err := r.resolveRequiredString(sc.Threshold, "threshold")
			if err != nil {
				return nil, err
			}
			oldThreshold, err := r.resolveRequiredInt16(sc.ThresholdOld, "oldThreshold")
			if err != nil {
				return nil, err
			}
			newThreshold, err := r.resolveRequiredInt16(sc.ThresholdNew, "newThreshold")
			if err != nil {
				return nil, err
			}
			return graphql1.SimulatedThresholdChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				Threshold:      types.ThresholdLevel(level),
				OldThreshold:   oldThreshold,
				NewThreshold:   newThreshold,
			}, nil
		default: // invalid reason for SIGNATURE_THRESHOLD; falls through to the error below
		}
	case types.StateChangeCategoryFlags:
		switch sc.StateChangeReason {
		case types.StateChangeReasonSet, types.StateChangeReasonClear:
			// A FLAGS row is only emitted when at least one flag changed, so a
			// missing flags value is a data error, not an empty list.
			if !sc.Flags.Valid {
				return nil, fmt.Errorf("state change is missing required flags")
			}
			return graphql1.SimulatedAccountFlagsChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				Flags:          types.DecodeAccountFlags(sc.Flags.Int16),
			}, nil
		default: // invalid reason for FLAGS; falls through to the error below
		}
	case types.StateChangeCategoryHomeDomain:
		oldDomain := flatKeyValueString(sc.KeyValue, "old")
		newDomain := flatKeyValueString(sc.KeyValue, "new")
		switch sc.StateChangeReason {
		case types.StateChangeReasonSet:
			if newDomain == nil {
				return nil, fmt.Errorf("state change is missing required homeDomain")
			}
			return graphql1.SimulatedHomeDomainSetChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				HomeDomain:     *newDomain,
			}, nil
		case types.StateChangeReasonUpdate:
			if oldDomain == nil || newDomain == nil {
				return nil, fmt.Errorf("state change is missing required oldHomeDomain/newHomeDomain")
			}
			return graphql1.SimulatedHomeDomainUpdatedChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				OldHomeDomain:  *oldDomain,
				NewHomeDomain:  *newDomain,
			}, nil
		case types.StateChangeReasonClear:
			if oldDomain == nil {
				return nil, fmt.Errorf("state change is missing required oldHomeDomain")
			}
			return graphql1.SimulatedHomeDomainClearedChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				OldHomeDomain:  *oldDomain,
			}, nil
		default: // invalid reason for HOME_DOMAIN; falls through to the error below
		}
	case types.StateChangeCategoryDataEntry:
		name, err := r.resolveRequiredString(sc.DataEntryName, "name")
		if err != nil {
			return nil, err
		}
		oldValue := flatKeyValueString(sc.KeyValue, "old")
		newValue := flatKeyValueString(sc.KeyValue, "new")
		switch sc.StateChangeReason {
		case types.StateChangeReasonAdd:
			if newValue == nil {
				return nil, fmt.Errorf("state change is missing required value")
			}
			return graphql1.SimulatedDataEntryAddedChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				Name:           name,
				Value:          *newValue,
			}, nil
		case types.StateChangeReasonUpdate:
			if oldValue == nil || newValue == nil {
				return nil, fmt.Errorf("state change is missing required oldValue/newValue")
			}
			return graphql1.SimulatedDataEntryUpdatedChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				Name:           name,
				OldValue:       *oldValue,
				NewValue:       *newValue,
			}, nil
		case types.StateChangeReasonRemove:
			if oldValue == nil {
				return nil, fmt.Errorf("state change is missing required oldValue")
			}
			return graphql1.SimulatedDataEntryRemovedChange{
				Category:       sc.StateChangeCategory,
				Reason:         sc.StateChangeReason,
				AccountAddress: accountAddress,
				Name:           name,
				OldValue:       *oldValue,
			}, nil
		default: // invalid reason for DATA_ENTRY; falls through to the error below
		}
	case types.StateChangeCategoryTrustline:
		tokenID := r.resolveNullableAddress(sc.TokenID)
		liquidityPoolID := r.resolveNullableString(sc.LiquidityPoolID)
		switch sc.StateChangeReason {
		case types.StateChangeReasonAdd:
			limit, err := r.resolveRequiredString(sc.TrustlineLimitNew, "limit")
			if err != nil {
				return nil, err
			}
			return graphql1.SimulatedTrustlineAddedChange{
				Category:        sc.StateChangeCategory,
				Reason:          sc.StateChangeReason,
				AccountAddress:  accountAddress,
				TokenID:         tokenID,
				LiquidityPoolID: liquidityPoolID,
				Limit:           limit,
			}, nil
		case types.StateChangeReasonUpdate:
			oldLimit, err := r.resolveRequiredString(sc.TrustlineLimitOld, "oldLimit")
			if err != nil {
				return nil, err
			}
			newLimit, err := r.resolveRequiredString(sc.TrustlineLimitNew, "newLimit")
			if err != nil {
				return nil, err
			}
			return graphql1.SimulatedTrustlineUpdatedChange{
				Category:        sc.StateChangeCategory,
				Reason:          sc.StateChangeReason,
				AccountAddress:  accountAddress,
				TokenID:         tokenID,
				LiquidityPoolID: liquidityPoolID,
				OldLimit:        oldLimit,
				NewLimit:        newLimit,
			}, nil
		case types.StateChangeReasonRemove:
			return graphql1.SimulatedTrustlineRemovedChange{
				Category:        sc.StateChangeCategory,
				Reason:          sc.StateChangeReason,
				AccountAddress:  accountAddress,
				TokenID:         tokenID,
				LiquidityPoolID: liquidityPoolID,
			}, nil
		default: // invalid reason for TRUSTLINE; falls through to the error below
		}
	}
	return nil, fmt.Errorf("state change has no simulated GraphQL type for (category=%s, reason=%s)",
		sc.StateChangeCategory, sc.StateChangeReason)
}

// keyValueUint32 reads a uint32 from a KeyValue payload. The value can arrive in
// two representations: a real uint32 when the state change is built in memory
// (the simulation path, e.g. sep41.Processor writes live_until_ledger directly),
// or a float64 when it has been round-tripped through JSONB from the database.
// Absent, wrong-typed, and out-of-range values each get their own error so a
// present-but-malformed value is not misreported as missing.
func keyValueUint32(kv types.NullableJSONB, key string) (uint32, error) {
	raw, ok := kv[key]
	if !ok {
		return 0, fmt.Errorf("state change is missing required %s", key)
	}
	switch v := raw.(type) {
	case uint32:
		return v, nil
	case float64:
		if v < 0 || v > math.MaxUint32 {
			return 0, fmt.Errorf("state change %s %v is out of uint32 range", key, v)
		}
		return uint32(v), nil
	default:
		return 0, fmt.Errorf("state change %s has unexpected type %T", key, raw)
	}
}
