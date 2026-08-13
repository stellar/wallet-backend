package processors

// This code was originally ported from Stellar Horizon's effects processing implementation.
// It processes transaction operations and generates effect outputs that align with Horizon's
// effects API format.

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"sort"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// EffectOutput is a representation of an operation that aligns with the BigQuery table history_effects
type EffectOutput struct {
	Address     string                 `json:"address"`
	OperationID int64                  `json:"operation_id"`
	Details     map[string]interface{} `json:"details"`
	Type        int32                  `json:"type"`
	TypeString  string                 `json:"type_string"`
}

// EffectType is the numeric type for an effect
type EffectType int

const (
	EffectAccountThresholdsUpdated EffectType = 4
	EffectAccountHomeDomainUpdated EffectType = 5
	EffectAccountFlagsUpdated      EffectType = 6
	EffectSignerCreated            EffectType = 10
	EffectSignerRemoved            EffectType = 11
	EffectSignerUpdated            EffectType = 12
	EffectTrustlineCreated         EffectType = 20
	EffectTrustlineRemoved         EffectType = 21
	EffectTrustlineUpdated         EffectType = 22
	EffectTrustlineFlagsUpdated    EffectType = 26
	EffectDataCreated              EffectType = 40
	EffectDataRemoved              EffectType = 41
	EffectDataUpdated              EffectType = 42
)

// EffectTypeNames stores a map of effect type ID and names
var EffectTypeNames = map[EffectType]string{
	EffectAccountThresholdsUpdated: "account_thresholds_updated",
	EffectAccountHomeDomainUpdated: "account_home_domain_updated",
	EffectAccountFlagsUpdated:      "account_flags_updated",
	EffectSignerCreated:            "signer_created",
	EffectSignerRemoved:            "signer_removed",
	EffectSignerUpdated:            "signer_updated",
	EffectTrustlineCreated:         "trustline_created",
	EffectTrustlineRemoved:         "trustline_removed",
	EffectTrustlineUpdated:         "trustline_updated",
	EffectTrustlineFlagsUpdated:    "trustline_flags_updated",
	EffectDataCreated:              "data_created",
	EffectDataRemoved:              "data_removed",
	EffectDataUpdated:              "data_updated",
}

// Effects returns the operation effects
func Effects(operation *TransactionOperationWrapper) ([]EffectOutput, error) {
	if !operation.Transaction.Result.Successful() {
		return []EffectOutput{}, nil
	}

	var err error
	wrapper := &effectsWrapper{
		effects:   []EffectOutput{},
		operation: operation,
	}

	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		wrapper.addAccountCreatedEffects()
	case xdr.OperationTypeSetOptions:
		err = wrapper.addSetOptionsEffects()
	case xdr.OperationTypeChangeTrust:
		err = wrapper.addChangeTrustEffects()
	case xdr.OperationTypeAllowTrust:
		wrapper.addAllowTrustEffects()
	case xdr.OperationTypeManageData:
		err = wrapper.addManageDataEffects()
	case xdr.OperationTypeSetTrustLineFlags:
		wrapper.addSetTrustLineFlagsEffects()
	default:
		// Skip operations that don't generate effects we track
	}
	if err != nil {
		return nil, err
	}

	return wrapper.effects, nil
}

type effectsWrapper struct {
	effects   []EffectOutput
	operation *TransactionOperationWrapper
}

func (e *effectsWrapper) add(address string, effectType EffectType, details map[string]interface{}) {
	e.effects = append(e.effects, EffectOutput{
		Address:     address,
		OperationID: e.operation.ID(),
		TypeString:  EffectTypeNames[effectType],
		Type:        int32(effectType),
		Details:     details,
	})
}

func (e *effectsWrapper) addUnmuxed(address *xdr.AccountId, effectType EffectType, details map[string]interface{}) {
	e.add(address.Address(), effectType, details)
}

func (e *effectsWrapper) addMuxed(address *xdr.MuxedAccount, effectType EffectType, details map[string]interface{}) {
	accID := address.ToAccountId()
	e.add(accID.Address(), effectType, details)
}

func (e *effectsWrapper) addAccountCreatedEffects() {
	op := e.operation.Operation.Body.MustCreateAccountOp()

	e.addUnmuxed(
		&op.Destination,
		EffectSignerCreated,
		map[string]interface{}{
			"public_key": op.Destination.Address(),
			"weight":     keypair.DefaultSignerWeight,
		},
	)
}

func (e *effectsWrapper) addSetOptionsEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustSetOptionsOp()

	if op.HomeDomain != nil {
		e.addMuxed(&source, EffectAccountHomeDomainUpdated,
			map[string]interface{}{
				"home_domain": string(*op.HomeDomain),
			},
		)
	}

	thresholdDetails := map[string]interface{}{}

	if op.LowThreshold != nil {
		thresholdDetails["low_threshold"] = *op.LowThreshold
	}

	if op.MedThreshold != nil {
		thresholdDetails["med_threshold"] = *op.MedThreshold
	}

	if op.HighThreshold != nil {
		thresholdDetails["high_threshold"] = *op.HighThreshold
	}

	if len(thresholdDetails) > 0 {
		e.addMuxed(&source, EffectAccountThresholdsUpdated, thresholdDetails)
	}

	flagDetails := map[string]interface{}{}
	if op.SetFlags != nil {
		setAuthFlagDetails(flagDetails, xdr.AccountFlags(*op.SetFlags), true)
	}
	if op.ClearFlags != nil {
		setAuthFlagDetails(flagDetails, xdr.AccountFlags(*op.ClearFlags), false)
	}

	if len(flagDetails) > 0 {
		e.addMuxed(&source, EffectAccountFlagsUpdated, flagDetails)
	}

	changes, err := e.operation.Changes()
	if err != nil {
		return fmt.Errorf("getting operation changes: %w", err)
	}

	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		beforeAccount := change.Pre.Data.MustAccount()
		afterAccount := change.Post.Data.MustAccount()

		before := beforeAccount.SignerSummary()
		after := afterAccount.SignerSummary()

		// if before and after are the same, the signers have not changed
		if reflect.DeepEqual(before, after) {
			continue
		}

		beforeSortedSigners := []string{}
		for signer := range before {
			beforeSortedSigners = append(beforeSortedSigners, signer)
		}
		sort.Strings(beforeSortedSigners)

		for _, addy := range beforeSortedSigners {
			weight, ok := after[addy]
			if !ok {
				e.addMuxed(&source, EffectSignerRemoved, map[string]interface{}{
					"public_key": addy,
				})
				continue
			}

			if weight != before[addy] {
				e.addMuxed(&source, EffectSignerUpdated, map[string]interface{}{
					"public_key": addy,
					"weight":     weight,
				})
			}
		}

		afterSortedSigners := []string{}
		for signer := range after {
			afterSortedSigners = append(afterSortedSigners, signer)
		}
		sort.Strings(afterSortedSigners)

		// Add the "created" effects
		for _, addy := range afterSortedSigners {
			weight := after[addy]
			// if `addy` is in before, the previous for loop should have recorded
			// the update, so skip this key
			if _, ok := before[addy]; ok {
				continue
			}

			e.addMuxed(&source, EffectSignerCreated, map[string]interface{}{
				"public_key": addy,
				"weight":     weight,
			})
		}
	}
	return nil
}

func (e *effectsWrapper) addChangeTrustEffects() error {
	source := e.operation.SourceAccount()

	op := e.operation.Operation.Body.MustChangeTrustOp()
	changes, err := e.operation.Changes()
	if err != nil {
		return fmt.Errorf("getting operation changes: %w", err)
	}

	// NOTE:  when an account trusts itself, the transaction is successful but
	// no ledger entries are actually modified.
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		var (
			effect    EffectType
			trustLine xdr.TrustLineEntry
		)

		switch {
		case change.Pre == nil && change.Post != nil:
			effect = EffectTrustlineCreated
			trustLine = *change.Post.Data.TrustLine
		case change.Pre != nil && change.Post == nil:
			effect = EffectTrustlineRemoved
			trustLine = *change.Pre.Data.TrustLine
		case change.Pre != nil && change.Post != nil:
			effect = EffectTrustlineUpdated
			trustLine = *change.Post.Data.TrustLine
		default:
			panic("Invalid change")
		}

		// We want to add a single effect for change_trust op. If it's modifying
		// credit_asset search for credit_asset trustline, otherwise search for
		// liquidity_pool.
		if op.Line.Type != trustLine.Asset.Type {
			continue
		}

		details := map[string]interface{}{"limit": amount.String(op.Limit)}
		if trustLine.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			// The only change_trust ops that can modify LP are those with
			// asset=liquidity_pool so *op.Line.LiquidityPool below is available.
			if addErr := AddLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); addErr != nil {
				return fmt.Errorf("adding liquidity pool asset details: %w", addErr)
			}
		} else {
			if addErr := AddAssetDetails(details, op.Line.ToAsset(), ""); addErr != nil {
				return fmt.Errorf("adding asset details: %w", addErr)
			}
		}

		e.addMuxed(&source, effect, details)
		break
	}

	return nil
}

func (e *effectsWrapper) addAllowTrustEffects() {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustAllowTrustOp()
	asset := op.Asset.ToAsset(source.ToAccountId())
	details := map[string]interface{}{
		"trustor": op.Trustor.Address(),
	}
	err := AddAssetDetails(details, asset, "")
	if err != nil {
		panic(fmt.Errorf("failed to add asset details: %w", err))
	}

	switch {
	case xdr.TrustLineFlags(op.Authorize).IsAuthorized():
		e.addMuxed(&source, EffectTrustlineFlagsUpdated, details)
		// Forward compatibility
		setFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
		e.addTrustLineFlagsEffect(&source, &op.Trustor, asset, &setFlags, nil)
	case xdr.TrustLineFlags(op.Authorize).IsAuthorizedToMaintainLiabilitiesFlag():
		e.addMuxed(
			&source,
			EffectTrustlineFlagsUpdated,
			details,
		)
		// Forward compatibility
		setFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
		e.addTrustLineFlagsEffect(&source, &op.Trustor, asset, &setFlags, nil)
	default:
		e.addMuxed(&source, EffectTrustlineFlagsUpdated, details)
		// Forward compatibility, show both as cleared
		clearFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag | xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
		e.addTrustLineFlagsEffect(&source, &op.Trustor, asset, nil, &clearFlags)
	}
}

func (e *effectsWrapper) addManageDataEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustManageDataOp()
	details := map[string]interface{}{"name": op.DataName}
	effect := EffectType(0)
	changes, err := e.operation.Changes()
	if err != nil {
		return fmt.Errorf("getting operation changes: %w", err)
	}

	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeData {
			continue
		}

		before := change.Pre
		after := change.Post

		if after != nil {
			raw := after.Data.MustData().DataValue
			details["value"] = base64.StdEncoding.EncodeToString(raw)
		}

		switch {
		case before == nil && after != nil:
			effect = EffectDataCreated
		case before != nil && after == nil:
			effect = EffectDataRemoved
		case before != nil && after != nil:
			effect = EffectDataUpdated
		default:
			panic("Invalid before-and-after state")
		}

		break
	}

	e.addMuxed(&source, effect, details)
	return nil
}

func (e *effectsWrapper) addSetTrustLineFlagsEffects() {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustSetTrustLineFlagsOp()
	e.addTrustLineFlagsEffect(&source, &op.Trustor, op.Asset, &op.SetFlags, &op.ClearFlags)
}

func (e *effectsWrapper) addTrustLineFlagsEffect(
	account *xdr.MuxedAccount,
	trustor *xdr.AccountId,
	asset xdr.Asset,
	setFlags *xdr.Uint32,
	clearFlags *xdr.Uint32,
) {
	details := map[string]interface{}{
		"trustor": trustor.Address(),
	}
	err := AddAssetDetails(details, asset, "")
	if err != nil {
		panic(fmt.Errorf("failed to add asset details: %w", err))
	}

	var flagDetailsAdded bool
	if setFlags != nil {
		setTrustLineFlagDetails(details, xdr.TrustLineFlags(*setFlags), true)
		flagDetailsAdded = true
	}
	if clearFlags != nil {
		setTrustLineFlagDetails(details, xdr.TrustLineFlags(*clearFlags), false)
		flagDetailsAdded = true
	}

	if flagDetailsAdded {
		e.addMuxed(account, EffectTrustlineFlagsUpdated, details)
	}
}

func setTrustLineFlagDetails(flagDetails map[string]interface{}, flags xdr.TrustLineFlags, setValue bool) {
	if flags.IsAuthorized() {
		flagDetails["authorized"] = setValue
	}
	if flags.IsAuthorizedToMaintainLiabilitiesFlag() {
		flagDetails["authorized_to_maintain_liabilities"] = setValue
	}
	if flags.IsClawbackEnabledFlag() {
		flagDetails["clawback_enabled"] = setValue
	}
}

func setAuthFlagDetails(flagDetails map[string]interface{}, flags xdr.AccountFlags, setValue bool) {
	if flags.IsAuthRequired() {
		flagDetails["auth_required"] = setValue
	}
	if flags.IsAuthRevocable() {
		flagDetails["auth_revocable"] = setValue
	}
	if flags.IsAuthImmutable() {
		flagDetails["auth_immutable"] = setValue
	}
	if flags.IsAuthClawbackEnabled() {
		flagDetails["auth_clawback_enabled"] = setValue
	}
}
