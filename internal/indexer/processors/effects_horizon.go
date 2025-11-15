package processors

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/guregu/null"
	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// EffectOutput is a representation of an operation that aligns with the BigQuery table history_effects
type EffectOutput struct {
	Address        string                 `json:"address"`
	AddressMuxed   null.String            `json:"address_muxed,omitempty"`
	OperationID    int64                  `json:"operation_id"`
	Details        map[string]interface{} `json:"details"`
	Type           int32                  `json:"type"`
	TypeString     string                 `json:"type_string"`
	LedgerClosed   time.Time              `json:"closed_at"`
	LedgerSequence uint32                 `json:"ledger_sequence"`
	EffectIndex    uint32                 `json:"index"`
	EffectID       string                 `json:"id"`
}

// EffectType is the numeric type for an effect
type EffectType int

const (
	EffectAccountThresholdsUpdated           EffectType = 4
	EffectAccountHomeDomainUpdated           EffectType = 5
	EffectAccountFlagsUpdated                EffectType = 6
	EffectSignerCreated                      EffectType = 10
	EffectSignerRemoved                      EffectType = 11
	EffectSignerUpdated                      EffectType = 12
	EffectTrustlineCreated                   EffectType = 20
	EffectTrustlineRemoved                   EffectType = 21
	EffectTrustlineUpdated                   EffectType = 22
	EffectTrustlineFlagsUpdated              EffectType = 26
	EffectDataCreated                        EffectType = 40
	EffectDataRemoved                        EffectType = 41
	EffectDataUpdated                        EffectType = 42
	EffectAccountSponsorshipCreated          EffectType = 60
	EffectAccountSponsorshipUpdated          EffectType = 61
	EffectAccountSponsorshipRemoved          EffectType = 62
	EffectTrustlineSponsorshipCreated        EffectType = 63
	EffectTrustlineSponsorshipUpdated        EffectType = 64
	EffectTrustlineSponsorshipRemoved        EffectType = 65
	EffectDataSponsorshipCreated             EffectType = 66
	EffectDataSponsorshipUpdated             EffectType = 67
	EffectDataSponsorshipRemoved             EffectType = 68
	EffectClaimableBalanceSponsorshipCreated EffectType = 69
	EffectClaimableBalanceSponsorshipUpdated EffectType = 70
	EffectClaimableBalanceSponsorshipRemoved EffectType = 71
	EffectSignerSponsorshipCreated           EffectType = 72
	EffectSignerSponsorshipUpdated           EffectType = 73
	EffectSignerSponsorshipRemoved           EffectType = 74
	EffectLiquidityPoolRevoked               EffectType = 95
)

// EffectTypeNames stores a map of effect type ID and names
var EffectTypeNames = map[EffectType]string{
	EffectAccountThresholdsUpdated:           "account_thresholds_updated",
	EffectAccountHomeDomainUpdated:           "account_home_domain_updated",
	EffectAccountFlagsUpdated:                "account_flags_updated",
	EffectSignerCreated:                      "signer_created",
	EffectSignerRemoved:                      "signer_removed",
	EffectSignerUpdated:                      "signer_updated",
	EffectTrustlineCreated:                   "trustline_created",
	EffectTrustlineRemoved:                   "trustline_removed",
	EffectTrustlineUpdated:                   "trustline_updated",
	EffectTrustlineFlagsUpdated:              "trustline_flags_updated",
	EffectDataCreated:                        "data_created",
	EffectDataRemoved:                        "data_removed",
	EffectDataUpdated:                        "data_updated",
	EffectAccountSponsorshipCreated:          "account_sponsorship_created",
	EffectAccountSponsorshipUpdated:          "account_sponsorship_updated",
	EffectAccountSponsorshipRemoved:          "account_sponsorship_removed",
	EffectTrustlineSponsorshipCreated:        "trustline_sponsorship_created",
	EffectTrustlineSponsorshipUpdated:        "trustline_sponsorship_updated",
	EffectTrustlineSponsorshipRemoved:        "trustline_sponsorship_removed",
	EffectDataSponsorshipCreated:             "data_sponsorship_created",
	EffectDataSponsorshipUpdated:             "data_sponsorship_updated",
	EffectDataSponsorshipRemoved:             "data_sponsorship_removed",
	EffectClaimableBalanceSponsorshipCreated: "claimable_balance_sponsorship_created",
	EffectClaimableBalanceSponsorshipUpdated: "claimable_balance_sponsorship_updated",
	EffectClaimableBalanceSponsorshipRemoved: "claimable_balance_sponsorship_removed",
	EffectSignerSponsorshipCreated:           "signer_sponsorship_created",
	EffectSignerSponsorshipUpdated:           "signer_sponsorship_updated",
	EffectSignerSponsorshipRemoved:           "signer_sponsorship_removed",
	EffectLiquidityPoolRevoked:               "liquidity_pool_revoked",
}

// Effects returns the operation effects
func Effects(operation *TransactionOperationWrapper) ([]EffectOutput, error) {
	if !operation.Transaction.Result.Successful() {
		return []EffectOutput{}, nil
	}

	changes, err := operation.Transaction.GetOperationChanges(operation.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	wrapper := &effectsWrapper{
		effects:   []EffectOutput{},
		operation: operation,
	}

	switch operation.OperationType() {
	case xdr.OperationTypeSetOptions:
		err = wrapper.addSetOptionsEffects()
	case xdr.OperationTypeChangeTrust:
		err = wrapper.addChangeTrustEffects()
	case xdr.OperationTypeAllowTrust:
		wrapper.addAllowTrustEffects()
	case xdr.OperationTypeManageData:
		err = wrapper.addManageDataEffects()
	case xdr.OperationTypeBeginSponsoringFutureReserves, xdr.OperationTypeEndSponsoringFutureReserves, xdr.OperationTypeRevokeSponsorship:
	// The effects of these operations are obtained  indirectly from the ledger entries
	case xdr.OperationTypeSetTrustLineFlags:
		wrapper.addSetTrustLineFlagsEffects()
	default:
		// Skip operations that don't generate effects we track
	}
	if err != nil {
		return nil, err
	}

	// Effects generated for multiple operations. Keep the effect categories
	// separated so they are "together" in case of different order or meta
	// changes generate by core (unordered_map).

	// Sponsorships
	for _, change := range changes {
		if err = wrapper.addLedgerEntrySponsorshipEffects(change); err != nil {
			return nil, err
		}
		wrapper.addSignerSponsorshipEffects(change)
	}

	for i := range wrapper.effects {
		wrapper.effects[i].LedgerClosed = operation.LedgerClosed
		wrapper.effects[i].LedgerSequence = operation.LedgerSequence
		wrapper.effects[i].EffectIndex = uint32(i)
		wrapper.effects[i].EffectID = fmt.Sprintf("%d-%d", wrapper.effects[i].OperationID, wrapper.effects[i].EffectIndex)
	}

	return wrapper.effects, nil
}

type effectsWrapper struct {
	effects   []EffectOutput
	operation *TransactionOperationWrapper
}

func (e *effectsWrapper) add(address string, addressMuxed null.String, effectType EffectType, details map[string]interface{}) {
	e.effects = append(e.effects, EffectOutput{
		Address:      address,
		AddressMuxed: addressMuxed,
		OperationID:  e.operation.ID(),
		TypeString:   EffectTypeNames[effectType],
		Type:         int32(effectType),
		Details:      details,
	})
}

func (e *effectsWrapper) addUnmuxed(address *xdr.AccountId, effectType EffectType, details map[string]interface{}) {
	e.add(address.Address(), null.String{}, effectType, details)
}

func (e *effectsWrapper) addMuxed(address *xdr.MuxedAccount, effectType EffectType, details map[string]interface{}) {
	var addressMuxed null.String
	if address.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		addressMuxed = null.StringFrom(address.Address())
	}
	accID := address.ToAccountId()
	e.add(accID.Address(), addressMuxed, effectType, details)
}

var sponsoringEffectsTable = map[xdr.LedgerEntryType]struct {
	created, updated, removed EffectType
}{
	xdr.LedgerEntryTypeAccount: {
		created: EffectAccountSponsorshipCreated,
		updated: EffectAccountSponsorshipUpdated,
		removed: EffectAccountSponsorshipRemoved,
	},
	xdr.LedgerEntryTypeTrustline: {
		created: EffectTrustlineSponsorshipCreated,
		updated: EffectTrustlineSponsorshipUpdated,
		removed: EffectTrustlineSponsorshipRemoved,
	},
	xdr.LedgerEntryTypeData: {
		created: EffectDataSponsorshipCreated,
		updated: EffectDataSponsorshipUpdated,
		removed: EffectDataSponsorshipRemoved,
	},
	xdr.LedgerEntryTypeClaimableBalance: {
		created: EffectClaimableBalanceSponsorshipCreated,
		updated: EffectClaimableBalanceSponsorshipUpdated,
		removed: EffectClaimableBalanceSponsorshipRemoved,
	},

	// We intentionally don't have Sponsoring effects for Offer
	// entries because we don't generate creation effects for them.
}

func (e *effectsWrapper) addSignerSponsorshipEffects(change ingest.Change) {
	if change.Type != xdr.LedgerEntryTypeAccount {
		return
	}

	preSigners := map[string]xdr.AccountId{}
	postSigners := map[string]xdr.AccountId{}
	if change.Pre != nil {
		account := change.Pre.Data.MustAccount()
		preSigners = account.SponsorPerSigner()
	}
	if change.Post != nil {
		account := change.Post.Data.MustAccount()
		postSigners = account.SponsorPerSigner()
	}

	var all []string
	for signer := range preSigners {
		all = append(all, signer)
	}
	for signer := range postSigners {
		if _, ok := preSigners[signer]; ok {
			continue
		}
		all = append(all, signer)
	}
	sort.Strings(all)

	for _, signer := range all {
		pre, foundPre := preSigners[signer]
		post, foundPost := postSigners[signer]
		details := map[string]interface{}{}

		switch {
		case !foundPre && !foundPost:
			continue
		case !foundPre && foundPost:
			details["sponsor"] = post.Address()
			details["signer"] = signer
			srcAccount := change.Post.Data.MustAccount().AccountId
			e.addUnmuxed(&srcAccount, EffectSignerSponsorshipCreated, details)
		case !foundPost && foundPre:
			details["former_sponsor"] = pre.Address()
			details["signer"] = signer
			srcAccount := change.Pre.Data.MustAccount().AccountId
			e.addUnmuxed(&srcAccount, EffectSignerSponsorshipRemoved, details)
		case foundPre && foundPost:
			formerSponsor := pre.Address()
			newSponsor := post.Address()
			if formerSponsor == newSponsor {
				continue
			}

			details["former_sponsor"] = formerSponsor
			details["new_sponsor"] = newSponsor
			details["signer"] = signer
			srcAccount := change.Post.Data.MustAccount().AccountId
			e.addUnmuxed(&srcAccount, EffectSignerSponsorshipUpdated, details)
		}
	}
}

func (e *effectsWrapper) addLedgerEntrySponsorshipEffects(change ingest.Change) error {
	effectsForEntryType, found := sponsoringEffectsTable[change.Type]
	if !found {
		return nil
	}

	details := map[string]interface{}{}
	var effectType EffectType

	switch {
	case (change.Pre == nil || change.Pre.SponsoringID() == nil) &&
		(change.Post != nil && change.Post.SponsoringID() != nil):
		effectType = effectsForEntryType.created
		details["sponsor"] = (*change.Post.SponsoringID()).Address()
	case (change.Pre != nil && change.Pre.SponsoringID() != nil) &&
		(change.Post == nil || change.Post.SponsoringID() == nil):
		effectType = effectsForEntryType.removed
		details["former_sponsor"] = (*change.Pre.SponsoringID()).Address()
	case (change.Pre != nil && change.Pre.SponsoringID() != nil) &&
		(change.Post != nil && change.Post.SponsoringID() != nil):
		preSponsor := (*change.Pre.SponsoringID()).Address()
		postSponsor := (*change.Post.SponsoringID()).Address()
		if preSponsor == postSponsor {
			return nil
		}
		effectType = effectsForEntryType.updated
		details["new_sponsor"] = postSponsor
		details["former_sponsor"] = preSponsor
	default:
		return nil
	}

	var (
		accountID    *xdr.AccountId
		muxedAccount *xdr.MuxedAccount
	)

	var data xdr.LedgerEntryData
	if change.Post != nil {
		data = change.Post.Data
	} else {
		data = change.Pre.Data
	}

	switch change.Type {
	case xdr.LedgerEntryTypeAccount:
		a := data.MustAccount().AccountId
		accountID = &a
	case xdr.LedgerEntryTypeTrustline:
		tl := data.MustTrustLine()
		accountID = &tl.AccountId
		if tl.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			details["asset_type"] = "liquidity_pool"
			details["liquidity_pool_id"] = PoolIDToString(*tl.Asset.LiquidityPoolId)
		} else {
			details["asset"] = tl.Asset.ToAsset().StringCanonical()
		}
	case xdr.LedgerEntryTypeData:
		muxedAccount = e.operation.SourceAccount()
		details["data_name"] = data.MustData().DataName
	case xdr.LedgerEntryTypeClaimableBalance:
		muxedAccount = e.operation.SourceAccount()
		var err error
		details["balance_id"], err = xdr.MarshalHex(data.MustClaimableBalance().BalanceId)
		if err != nil {
			return errors.Wrapf(err, "Invalid balanceId in change from op %d", e.operation.Index)
		}
	case xdr.LedgerEntryTypeLiquidityPool:
		// liquidity pools cannot be sponsored
		fallthrough
	default:
		return errors.Errorf("invalid sponsorship ledger entry type %v", change.Type.String())
	}

	if accountID != nil {
		e.addUnmuxed(accountID, effectType, details)
	} else {
		e.addMuxed(muxedAccount, effectType, details)
	}

	return nil
}

func (e *effectsWrapper) addSetOptionsEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustSetOptionsOp()

	if op.HomeDomain != nil {
		e.addMuxed(source, EffectAccountHomeDomainUpdated,
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
		e.addMuxed(source, EffectAccountThresholdsUpdated, thresholdDetails)
	}

	flagDetails := map[string]interface{}{}
	if op.SetFlags != nil {
		setAuthFlagDetails(flagDetails, xdr.AccountFlags(*op.SetFlags), true)
	}
	if op.ClearFlags != nil {
		setAuthFlagDetails(flagDetails, xdr.AccountFlags(*op.ClearFlags), false)
	}

	if len(flagDetails) > 0 {
		e.addMuxed(source, EffectAccountFlagsUpdated, flagDetails)
	}

	changes, err := e.operation.Transaction.GetOperationChanges(e.operation.Index)
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
				e.addMuxed(source, EffectSignerRemoved, map[string]interface{}{
					"public_key": addy,
				})
				continue
			}

			if weight != before[addy] {
				e.addMuxed(source, EffectSignerUpdated, map[string]interface{}{
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

			e.addMuxed(source, EffectSignerCreated, map[string]interface{}{
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
	changes, err := e.operation.Transaction.GetOperationChanges(e.operation.Index)
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
			if addErr := addLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); addErr != nil {
				return fmt.Errorf("adding liquidity pool asset details: %w", addErr)
			}
		} else {
			if addErr := addAssetDetails(details, op.Line.ToAsset(), ""); addErr != nil {
				return fmt.Errorf("adding asset details: %w", addErr)
			}
		}

		e.addMuxed(source, effect, details)
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
	err := addAssetDetails(details, asset, "")
	if err != nil {
		panic(fmt.Errorf("failed to add asset details: %w", err))
	}

	switch {
	case xdr.TrustLineFlags(op.Authorize).IsAuthorized():
		e.addMuxed(source, EffectTrustlineFlagsUpdated, details)
		// Forward compatibility
		setFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
		e.addTrustLineFlagsEffect(source, &op.Trustor, asset, &setFlags, nil)
	case xdr.TrustLineFlags(op.Authorize).IsAuthorizedToMaintainLiabilitiesFlag():
		e.addMuxed(
			source,
			EffectTrustlineFlagsUpdated,
			details,
		)
		// Forward compatibility
		setFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
		e.addTrustLineFlagsEffect(source, &op.Trustor, asset, &setFlags, nil)
	default:
		e.addMuxed(source, EffectTrustlineFlagsUpdated, details)
		// Forward compatibility, show both as cleared
		clearFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag | xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
		e.addTrustLineFlagsEffect(source, &op.Trustor, asset, nil, &clearFlags)
	}
}

func (e *effectsWrapper) addManageDataEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustManageDataOp()
	details := map[string]interface{}{"name": op.DataName}
	effect := EffectType(0)
	changes, err := e.operation.Transaction.GetOperationChanges(e.operation.Index)
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

	e.addMuxed(source, effect, details)
	return nil
}

func (e *effectsWrapper) addSetTrustLineFlagsEffects() {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustSetTrustLineFlagsOp()
	e.addTrustLineFlagsEffect(source, &op.Trustor, op.Asset, &op.SetFlags, &op.ClearFlags)
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
	err := addAssetDetails(details, asset, "")
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
		flagDetails["authorized_flag"] = setValue
	}
	if flags.IsAuthorizedToMaintainLiabilitiesFlag() {
		flagDetails["authorized_to_maintain_liabilites"] = setValue
	}
	if flags.IsClawbackEnabledFlag() {
		flagDetails["clawback_enabled_flag"] = setValue
	}
}

func setAuthFlagDetails(flagDetails map[string]interface{}, flags xdr.AccountFlags, setValue bool) {
	if flags.IsAuthRequired() {
		flagDetails["auth_required_flag"] = setValue
	}
	if flags.IsAuthRevocable() {
		flagDetails["auth_revocable_flag"] = setValue
	}
	if flags.IsAuthImmutable() {
		flagDetails["auth_immutable_flag"] = setValue
	}
	if flags.IsAuthClawbackEnabled() {
		flagDetails["auth_clawback_enabled_flag"] = setValue
	}
}
