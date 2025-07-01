package processors

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/stellar/go/ingest"
	effects "github.com/stellar/go/processors/effects"
	operation "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

var (
	thresholdToReasonMap = map[string]types.StateChangeReason{
		"low_threshold":  types.StateChangeReasonLow,
		"med_threshold":  types.StateChangeReasonMedium,
		"high_threshold": types.StateChangeReasonHigh,
	}
	signerEffectToReasonMap = map[int32]types.StateChangeReason{
		int32(effects.EffectSignerCreated): types.StateChangeReasonAdd,
		int32(effects.EffectSignerRemoved): types.StateChangeReasonRemove,
		int32(effects.EffectSignerUpdated): types.StateChangeReasonUpdate,
	}
	accountFlags = []string{
		"auth_required_flag",
		"auth_revocable_flag",
		"auth_immutable_flag",
		"auth_clawback_enabled_flag",
	}
	trustlineFlags = []string{
		"authorized_flag",
		"authorized_to_maintain_liabilites",
		"clawback_enabled_flag",
	}
)

type EffectsProcessor struct {
	networkPassphrase string
}

func NewEffectsProcessor(networkPassphrase string) *EffectsProcessor {
	return &EffectsProcessor{
		networkPassphrase: networkPassphrase,
	}
}

func (p *EffectsProcessor) ProcessTransaction(ctx context.Context, tx ingest.LedgerTransaction, op xdr.Operation, opIdx uint32) ([]types.StateChange, error) {
	ledgerCloseTime := tx.Ledger.LedgerCloseTime()
	ledgerNumber := tx.Ledger.LedgerSequence()
	opID := toid.New(int32(ledgerNumber), int32(tx.Index), int32(opIdx)).ToInt64()
	txID := toid.New(int32(ledgerNumber), int32(tx.Index), 0).ToInt64()

	opWrapper := operation.TransactionOperationWrapper{
		Index:          opIdx,
		LedgerSequence: tx.Ledger.LedgerSequence(),
		LedgerClosed:   time.Unix(tx.Ledger.LedgerCloseTime(), 0),
		Transaction:    tx,
		Operation:      op,
		Network:        p.networkPassphrase,
	}

	effectOutputs, err := effects.Effects(&opWrapper)
	if err != nil {
		return nil, fmt.Errorf("processing effects: %w", err)
	}

	stateChanges := make([]types.StateChange, 0)
	masterBuilder := NewStateChangeBuilder(ledgerNumber, ledgerCloseTime, txID).WithOperationID(opID)
	for _, effect := range effectOutputs {
		changeBuilder := masterBuilder.Clone().WithAccount(effect.Address)

		effectType := effects.EffectType(effect.Type)
		//exhaustive:ignore
		switch effectType {
		case effects.EffectSignerCreated, effects.EffectSignerRemoved, effects.EffectSignerUpdated:
			signerWeight := effect.Details["weight"]
			signerPublicKey := effect.Details["public_key"]
			stateChanges = append(stateChanges, changeBuilder.
				WithCategory(types.StateChangeCategorySigner).
				WithReason(signerEffectToReasonMap[effect.Type]).
				WithSigner(signerPublicKey.(string), signerWeight).
				Build())

		case effects.EffectAccountThresholdsUpdated:
			changeBuilder = changeBuilder.WithCategory(types.StateChangeCategorySignatureThreshold)
			stateChanges = append(stateChanges, p.parseThresholds(changeBuilder, &effect)...)

		case effects.EffectAccountFlagsUpdated:
			changeBuilder = changeBuilder.WithCategory(types.StateChangeCategoryFlags)
			stateChanges = append(stateChanges, p.parseFlags(accountFlags, changeBuilder, &effect)...)

		case effects.EffectAccountHomeDomainUpdated:
			keyValueMap := p.parseKeyValue([]string{"home_domain"}, &effect)
			stateChanges = append(stateChanges, changeBuilder.
				WithCategory(types.StateChangeCategoryMetadata).
				WithReason(types.StateChangeReasonHomeDomain).
				WithKeyValue(keyValueMap).
				Build())

		case effects.EffectTrustlineFlagsUpdated:
			changeBuilder = changeBuilder.WithCategory(types.StateChangeCategoryTrustlineFlags)
			stateChanges = append(stateChanges, p.parseFlags(trustlineFlags, changeBuilder, &effect)...)

		case effects.EffectDataCreated, effects.EffectDataRemoved, effects.EffectDataUpdated:
			keyValueMap := p.parseKeyValue([]string{"name", "value"}, &effect)
			stateChanges = append(stateChanges, changeBuilder.
				WithCategory(types.StateChangeCategoryMetadata).
				WithReason(types.StateChangeReasonDataEntry).
				WithKeyValue(keyValueMap).
				Build())

		case effects.EffectAccountSponsorshipCreated, effects.EffectAccountSponsorshipRemoved, effects.EffectAccountSponsorshipUpdated,
			effects.EffectClaimableBalanceSponsorshipCreated, effects.EffectClaimableBalanceSponsorshipRemoved, effects.EffectClaimableBalanceSponsorshipUpdated,
			effects.EffectDataSponsorshipCreated, effects.EffectDataSponsorshipRemoved, effects.EffectDataSponsorshipUpdated,
			effects.EffectSignerSponsorshipCreated, effects.EffectSignerSponsorshipRemoved, effects.EffectSignerSponsorshipUpdated,
			effects.EffectTrustlineSponsorshipCreated, effects.EffectTrustlineSponsorshipRemoved, effects.EffectTrustlineSponsorshipUpdated:

			sponsorshipChanges := p.processSponsorshipEffect(effectType, effect, changeBuilder.Clone())
			stateChanges = append(stateChanges, sponsorshipChanges...)

		default:
			continue
		}
	}

	return stateChanges, nil
}

func (p *EffectsProcessor) processSponsorshipEffect(effectType effects.EffectType, effect effects.EffectOutput, baseBuilder *StateChangeBuilder) []types.StateChange {
	baseBuilder = baseBuilder.WithCategory(types.StateChangeCategorySponsorship)

	var sponsorChanges []types.StateChange
	var targetChange types.StateChange

	//exhaustive:ignore
	switch effectType {
	// Created cases
	case effects.EffectAccountSponsorshipCreated, effects.EffectClaimableBalanceSponsorshipCreated,
		effects.EffectDataSponsorshipCreated, effects.EffectSignerSponsorshipCreated, effects.EffectTrustlineSponsorshipCreated:
		sponsor := effect.Details["sponsor"].(string)
		sponsorChanges = append(sponsorChanges, p.createSponsorChange(types.StateChangeReasonSet, baseBuilder.Clone(), sponsor, effect.Address))
		targetChange = p.createTargetSponsorshipChange(types.StateChangeReasonSet, sponsor, effectType, effect, baseBuilder.Clone())

	// Removed cases
	case effects.EffectAccountSponsorshipRemoved, effects.EffectClaimableBalanceSponsorshipRemoved,
		effects.EffectDataSponsorshipRemoved, effects.EffectSignerSponsorshipRemoved, effects.EffectTrustlineSponsorshipRemoved:
		formerSponsor := effect.Details["former_sponsor"].(string)
		sponsorChanges = append(sponsorChanges, p.createSponsorChange(types.StateChangeReasonRevoke, baseBuilder.Clone(), formerSponsor, effect.Address))
		targetChange = p.createTargetSponsorshipChange(types.StateChangeReasonRevoke, formerSponsor, effectType, effect, baseBuilder.Clone())

	// Updated cases
	case effects.EffectAccountSponsorshipUpdated, effects.EffectClaimableBalanceSponsorshipUpdated,
		effects.EffectDataSponsorshipUpdated, effects.EffectSignerSponsorshipUpdated, effects.EffectTrustlineSponsorshipUpdated:
		newSponsor := effect.Details["new_sponsor"].(string)
		formerSponsor := effect.Details["former_sponsor"].(string)
		sponsorChanges = append(sponsorChanges,
			p.createSponsorChange(types.StateChangeReasonSet, baseBuilder.Clone(), newSponsor, effect.Address),
			p.createSponsorChange(types.StateChangeReasonRemove, baseBuilder.Clone(), formerSponsor, effect.Address),
		)
		targetBuilder := baseBuilder.Clone().WithKeyValue(p.parseKeyValue([]string{"former_sponsor"}, &effect))
		targetChange = p.createTargetSponsorshipChange(types.StateChangeReasonUpdate, newSponsor, effectType, effect, targetBuilder)
	}

	return append(sponsorChanges, targetChange)
}

func (p *EffectsProcessor) createSponsorChange(reason types.StateChangeReason, builder *StateChangeBuilder, sponsor string, targetAccountID string) types.StateChange {
	return builder.
		WithReason(reason).
		WithAccount(sponsor).
		WithTargetAccountID(targetAccountID).
		Build()
}

func (p *EffectsProcessor) createTargetSponsorshipChange(reason types.StateChangeReason, sponsor string, effectType effects.EffectType, effect effects.EffectOutput, builder *StateChangeBuilder) types.StateChange {
	builder = builder.WithReason(reason).WithSponsor(sponsor)

	//exhaustive:ignore
	switch effectType {
	case effects.EffectClaimableBalanceSponsorshipCreated, effects.EffectClaimableBalanceSponsorshipUpdated, effects.EffectClaimableBalanceSponsorshipRemoved:
		builder = builder.WithClaimableBalance(effect.Details["balance_id"].(string))
	case effects.EffectTrustlineSponsorshipCreated, effects.EffectTrustlineSponsorshipUpdated, effects.EffectTrustlineSponsorshipRemoved:
		if lpID, ok := effect.Details["liquidity_pool_id"]; ok {
			builder = builder.WithLiquidityPool(lpID.(string))
		}
	case effects.EffectSignerSponsorshipCreated, effects.EffectSignerSponsorshipUpdated, effects.EffectSignerSponsorshipRemoved:
		builder = builder.WithSigner(effect.Details["signer"].(string), 0)
	}

	return builder.Build()
}

func (p *EffectsProcessor) parseKeyValue(keys []string, effect *effects.EffectOutput) map[string]any {
	keyValueMap := map[string]any{}
	for _, key := range keys {
		if value, ok := effect.Details[key]; ok {
			keyValueMap[key] = value
		}
	}
	return keyValueMap
}

func (p *EffectsProcessor) parseFlags(flags []string, changeBuilder *StateChangeBuilder, effect *effects.EffectOutput) []types.StateChange {
	setFlags := make(map[string]any)
	clearFlags := make(map[string]any)
	for _, flag := range flags {
		if value, ok := effect.Details[flag]; ok {
			if value == true {
				setFlags[flag] = true
			} else {
				clearFlags[flag] = false
			}
		}
	}

	changes := make([]types.StateChange, 0)
	if len(setFlags) > 0 {
		changes = append(changes, changeBuilder.
			Clone().
			WithReason(types.StateChangeReasonSet).
			WithFlags(setFlags).
			Build())
	}
	if len(clearFlags) > 0 {
		changes = append(changes, changeBuilder.
			Clone().
			WithReason(types.StateChangeReasonClear).
			WithFlags(clearFlags).
			Build())
	}

	return changes
}

func (p *EffectsProcessor) parseThresholds(changeBuilder *StateChangeBuilder, effect *effects.EffectOutput) []types.StateChange {
	changes := make([]types.StateChange, 0)
	for threshold, reason := range thresholdToReasonMap {
		if value, ok := effect.Details[threshold]; ok {
			thresholdValue := strconv.FormatInt(int64(value.(xdr.Uint32)), 10)
			changes = append(changes, changeBuilder.
				Clone().
				WithReason(reason).
				WithThresholds(map[string]any{
					threshold: thresholdValue,
				}).
				Build())
		}
	}
	return changes
}
