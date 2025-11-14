package processors

import (
	"encoding/base64"
	"time"

	"fmt"
	"reflect"
	"sort"
	"strconv"

	"github.com/guregu/null"
	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/keypair"
	operations "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/processors/utils"
	"github.com/stellar/go/protocols/horizon/base"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/contractevents"
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
	EffectId       string                 `json:"id"`
}

// EffectType is the numeric type for an effect
type EffectType int

const (
	EffectAccountCreated                     EffectType = 0
	EffectAccountRemoved                     EffectType = 1
	EffectAccountCredited                    EffectType = 2
	EffectAccountDebited                     EffectType = 3
	EffectAccountThresholdsUpdated           EffectType = 4
	EffectAccountHomeDomainUpdated           EffectType = 5
	EffectAccountFlagsUpdated                EffectType = 6
	EffectAccountInflationDestinationUpdated EffectType = 7
	EffectSignerCreated                      EffectType = 10
	EffectSignerRemoved                      EffectType = 11
	EffectSignerUpdated                      EffectType = 12
	EffectTrustlineCreated                   EffectType = 20
	EffectTrustlineRemoved                   EffectType = 21
	EffectTrustlineUpdated                   EffectType = 22
	EffectTrustlineFlagsUpdated              EffectType = 26
	EffectOfferCreated                       EffectType = 30
	EffectOfferRemoved                       EffectType = 31
	EffectOfferUpdated                       EffectType = 32
	EffectTrade                              EffectType = 33
	EffectDataCreated                        EffectType = 40
	EffectDataRemoved                        EffectType = 41
	EffectDataUpdated                        EffectType = 42
	EffectSequenceBumped                     EffectType = 43
	EffectClaimableBalanceCreated            EffectType = 50
	EffectClaimableBalanceClaimantCreated    EffectType = 51
	EffectClaimableBalanceClaimed            EffectType = 52
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
	EffectClaimableBalanceClawedBack         EffectType = 80
	EffectLiquidityPoolDeposited             EffectType = 90
	EffectLiquidityPoolWithdrew              EffectType = 91
	EffectLiquidityPoolTrade                 EffectType = 92
	EffectLiquidityPoolCreated               EffectType = 93
	EffectLiquidityPoolRemoved               EffectType = 94
	EffectLiquidityPoolRevoked               EffectType = 95
	EffectContractCredited                   EffectType = 96
	EffectContractDebited                    EffectType = 97
	EffectExtendFootprintTtl                 EffectType = 98
	EffectRestoreFootprint                   EffectType = 99
)

// EffectTypeNames stores a map of effect type ID and names
var EffectTypeNames = map[EffectType]string{
	EffectAccountCreated:                     "account_created",
	EffectAccountRemoved:                     "account_removed",
	EffectAccountCredited:                    "account_credited",
	EffectAccountDebited:                     "account_debited",
	EffectAccountThresholdsUpdated:           "account_thresholds_updated",
	EffectAccountHomeDomainUpdated:           "account_home_domain_updated",
	EffectAccountFlagsUpdated:                "account_flags_updated",
	EffectAccountInflationDestinationUpdated: "account_inflation_destination_updated",
	EffectSignerCreated:                      "signer_created",
	EffectSignerRemoved:                      "signer_removed",
	EffectSignerUpdated:                      "signer_updated",
	EffectTrustlineCreated:                   "trustline_created",
	EffectTrustlineRemoved:                   "trustline_removed",
	EffectTrustlineUpdated:                   "trustline_updated",
	EffectTrustlineFlagsUpdated:              "trustline_flags_updated",
	EffectOfferCreated:                       "offer_created",
	EffectOfferRemoved:                       "offer_removed",
	EffectOfferUpdated:                       "offer_updated",
	EffectTrade:                              "trade",
	EffectDataCreated:                        "data_created",
	EffectDataRemoved:                        "data_removed",
	EffectDataUpdated:                        "data_updated",
	EffectSequenceBumped:                     "sequence_bumped",
	EffectClaimableBalanceCreated:            "claimable_balance_created",
	EffectClaimableBalanceClaimed:            "claimable_balance_claimed",
	EffectClaimableBalanceClaimantCreated:    "claimable_balance_claimant_created",
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
	EffectClaimableBalanceClawedBack:         "claimable_balance_clawed_back",
	EffectLiquidityPoolDeposited:             "liquidity_pool_deposited",
	EffectLiquidityPoolWithdrew:              "liquidity_pool_withdrew",
	EffectLiquidityPoolTrade:                 "liquidity_pool_trade",
	EffectLiquidityPoolCreated:               "liquidity_pool_created",
	EffectLiquidityPoolRemoved:               "liquidity_pool_removed",
	EffectLiquidityPoolRevoked:               "liquidity_pool_revoked",
	EffectContractCredited:                   "contract_credited",
	EffectContractDebited:                    "contract_debited",
	EffectExtendFootprintTtl:                 "extend_footprint_ttl",
	EffectRestoreFootprint:                   "restore_footprint",
}

func TransformEffect(transaction ingest.LedgerTransaction, ledgerSeq uint32, ledgerCloseMeta xdr.LedgerCloseMeta, networkPassphrase string) ([]EffectOutput, error) {
	effects := []EffectOutput{}

	outputCloseTime, err := utils.GetCloseTime(ledgerCloseMeta)
	if err != nil {
		return effects, err
	}

	for opi, op := range transaction.Envelope.Operations() {
		operation := operations.TransactionOperationWrapper{
			Index:          uint32(opi),
			Transaction:    transaction,
			Operation:      op,
			LedgerSequence: ledgerSeq,
			Network:        networkPassphrase,
			LedgerClosed:   outputCloseTime,
		}

		p, err := Effects(&operation)
		if err != nil {
			return effects, errors.Wrapf(err, "reading operation %v effects", operation.ID())
		}

		effects = append(effects, p...)

	}

	return effects, nil
}

// Effects returns the operation effects
func Effects(operation *operations.TransactionOperationWrapper) ([]EffectOutput, error) {
	if !operation.Transaction.Result.Successful() {
		return []EffectOutput{}, nil
	}
	var (
		op  = operation.Operation
		err error
	)

	changes, err := operation.Transaction.GetOperationChanges(operation.Index)
	if err != nil {
		return nil, err
	}

	wrapper := &effectsWrapper{
		effects:   []EffectOutput{},
		operation: operation,
	}

	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		wrapper.addAccountCreatedEffects()
	case xdr.OperationTypePayment:
		wrapper.addPaymentEffects()
	case xdr.OperationTypePathPaymentStrictReceive:
		err = wrapper.pathPaymentStrictReceiveEffects()
	case xdr.OperationTypePathPaymentStrictSend:
		err = wrapper.addPathPaymentStrictSendEffects()
	case xdr.OperationTypeManageSellOffer:
		err = wrapper.addManageSellOfferEffects()
	case xdr.OperationTypeManageBuyOffer:
		err = wrapper.addManageBuyOfferEffects()
	case xdr.OperationTypeCreatePassiveSellOffer:
		err = wrapper.addCreatePassiveSellOfferEffect()
	case xdr.OperationTypeSetOptions:
		wrapper.addSetOptionsEffects()
	case xdr.OperationTypeChangeTrust:
		err = wrapper.addChangeTrustEffects()
	case xdr.OperationTypeAllowTrust:
		err = wrapper.addAllowTrustEffects()
	case xdr.OperationTypeAccountMerge:
		wrapper.addAccountMergeEffects()
	case xdr.OperationTypeInflation:
		wrapper.addInflationEffects()
	case xdr.OperationTypeManageData:
		err = wrapper.addManageDataEffects()
	case xdr.OperationTypeBumpSequence:
		err = wrapper.addBumpSequenceEffects()
	case xdr.OperationTypeCreateClaimableBalance:
		err = wrapper.addCreateClaimableBalanceEffects(changes)
	case xdr.OperationTypeClaimClaimableBalance:
		err = wrapper.addClaimClaimableBalanceEffects(changes)
	case xdr.OperationTypeBeginSponsoringFutureReserves, xdr.OperationTypeEndSponsoringFutureReserves, xdr.OperationTypeRevokeSponsorship:
	// The effects of these operations are obtained  indirectly from the ledger entries
	case xdr.OperationTypeClawback:
		err = wrapper.addClawbackEffects()
	case xdr.OperationTypeClawbackClaimableBalance:
		err = wrapper.addClawbackClaimableBalanceEffects(changes)
	case xdr.OperationTypeSetTrustLineFlags:
		err = wrapper.addSetTrustLineFlagsEffects()
	case xdr.OperationTypeLiquidityPoolDeposit:
		err = wrapper.addLiquidityPoolDepositEffect()
	case xdr.OperationTypeLiquidityPoolWithdraw:
		err = wrapper.addLiquidityPoolWithdrawEffect()
	case xdr.OperationTypeInvokeHostFunction:
		// If there's an invokeHostFunction operation, there's definitely V3
		// meta in the transaction, which means this error is real.
		contractEvents, innerErr := operation.Transaction.GetContractEvents()
		if innerErr != nil {
			return nil, innerErr
		}

		// For now, the only effects are related to the events themselves.
		// Possible add'l work: https://github.com/stellar/go/issues/4585
		err = wrapper.addInvokeHostFunctionEffects(contractEvents)
	case xdr.OperationTypeExtendFootprintTtl:
		err = wrapper.addExtendFootprintTtlEffect()
	case xdr.OperationTypeRestoreFootprint:
		err = wrapper.addRestoreFootprintExpirationEffect()
	default:
		return nil, fmt.Errorf("unknown operation type: %s", op.Body.Type)
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

	// Liquidity pools
	for _, change := range changes {
		// Effects caused by ChangeTrust (creation), AllowTrust and SetTrustlineFlags (removal through revocation)
		wrapper.addLedgerEntryLiquidityPoolEffects(change)
	}

	for i := range wrapper.effects {
		wrapper.effects[i].LedgerClosed = operation.LedgerClosed
		wrapper.effects[i].LedgerSequence = operation.LedgerSequence
		wrapper.effects[i].EffectIndex = uint32(i)
		wrapper.effects[i].EffectId = fmt.Sprintf("%d-%d", wrapper.effects[i].OperationID, wrapper.effects[i].EffectIndex)
	}

	return wrapper.effects, nil
}

type effectsWrapper struct {
	effects   []EffectOutput
	operation *operations.TransactionOperationWrapper
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
			details["liquidity_pool_id"] = operations.PoolIDToString(*tl.Asset.LiquidityPoolId)
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

func (e *effectsWrapper) addLedgerEntryLiquidityPoolEffects(change ingest.Change) error {
	if change.Type != xdr.LedgerEntryTypeLiquidityPool {
		return nil
	}
	var effectType EffectType

	var details map[string]interface{}
	switch {
	case change.Pre == nil && change.Post != nil:
		effectType = EffectLiquidityPoolCreated
		details = map[string]interface{}{
			"liquidity_pool": liquidityPoolDetails(change.Post.Data.LiquidityPool),
		}
	case change.Pre != nil && change.Post == nil:
		effectType = EffectLiquidityPoolRemoved
		poolID := change.Pre.Data.LiquidityPool.LiquidityPoolId
		details = map[string]interface{}{
			"liquidity_pool_id": operations.PoolIDToString(poolID),
		}
	default:
		return nil
	}
	e.addMuxed(
		e.operation.SourceAccount(),
		effectType,
		details,
	)

	return nil
}

func (e *effectsWrapper) addAccountCreatedEffects() {
	op := e.operation.Operation.Body.MustCreateAccountOp()

	e.addUnmuxed(
		&op.Destination,
		EffectAccountCreated,
		map[string]interface{}{
			"starting_balance": amount.String(op.StartingBalance),
		},
	)
	e.addMuxed(
		e.operation.SourceAccount(),
		EffectAccountDebited,
		map[string]interface{}{
			"asset_type": "native",
			"amount":     amount.String(op.StartingBalance),
		},
	)
	e.addUnmuxed(
		&op.Destination,
		EffectSignerCreated,
		map[string]interface{}{
			"public_key": op.Destination.Address(),
			"weight":     keypair.DefaultSignerWeight,
		},
	)
}

func (e *effectsWrapper) addPaymentEffects() {
	op := e.operation.Operation.Body.MustPaymentOp()

	details := map[string]interface{}{"amount": amount.String(op.Amount)}
	operations.AddAssetDetails(details, op.Asset, "")

	e.addMuxed(
		&op.Destination,
		EffectAccountCredited,
		details,
	)
	e.addMuxed(
		e.operation.SourceAccount(),
		EffectAccountDebited,
		details,
	)
}

func (e *effectsWrapper) pathPaymentStrictReceiveEffects() error {
	op := e.operation.Operation.Body.MustPathPaymentStrictReceiveOp()
	resultSuccess := e.operation.OperationResult().MustPathPaymentStrictReceiveResult().MustSuccess()
	source := e.operation.SourceAccount()

	details := map[string]interface{}{"amount": amount.String(op.DestAmount)}
	operations.AddAssetDetails(details, op.DestAsset, "")

	e.addMuxed(
		&op.Destination,
		EffectAccountCredited,
		details,
	)

	result := e.operation.OperationResult().MustPathPaymentStrictReceiveResult()
	details = map[string]interface{}{"amount": amount.String(result.SendAmount())}
	operations.AddAssetDetails(details, op.SendAsset, "")

	e.addMuxed(
		source,
		EffectAccountDebited,
		details,
	)

	return e.addIngestTradeEffects(*source, resultSuccess.Offers, false)
}

func (e *effectsWrapper) addPathPaymentStrictSendEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustPathPaymentStrictSendOp()
	resultSuccess := e.operation.OperationResult().MustPathPaymentStrictSendResult().MustSuccess()
	result := e.operation.OperationResult().MustPathPaymentStrictSendResult()

	details := map[string]interface{}{"amount": amount.String(result.DestAmount())}
	operations.AddAssetDetails(details, op.DestAsset, "")
	e.addMuxed(&op.Destination, EffectAccountCredited, details)

	details = map[string]interface{}{"amount": amount.String(op.SendAmount)}
	operations.AddAssetDetails(details, op.SendAsset, "")
	e.addMuxed(source, EffectAccountDebited, details)

	return e.addIngestTradeEffects(*source, resultSuccess.Offers, true)
}

func (e *effectsWrapper) addManageSellOfferEffects() error {
	source := e.operation.SourceAccount()
	result := e.operation.OperationResult().MustManageSellOfferResult().MustSuccess()
	return e.addIngestTradeEffects(*source, result.OffersClaimed, false)
}

func (e *effectsWrapper) addManageBuyOfferEffects() error {
	source := e.operation.SourceAccount()
	result := e.operation.OperationResult().MustManageBuyOfferResult().MustSuccess()
	return e.addIngestTradeEffects(*source, result.OffersClaimed, false)
}

func (e *effectsWrapper) addCreatePassiveSellOfferEffect() error {
	result := e.operation.OperationResult()
	source := e.operation.SourceAccount()

	var claims []xdr.ClaimAtom

	// KNOWN ISSUE:  stellar-core creates results for CreatePassiveOffer operations
	// with the wrong result arm set.
	if result.Type == xdr.OperationTypeManageSellOffer {
		claims = result.MustManageSellOfferResult().MustSuccess().OffersClaimed
	} else {
		claims = result.MustCreatePassiveSellOfferResult().MustSuccess().OffersClaimed
	}

	return e.addIngestTradeEffects(*source, claims, false)
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

	if op.InflationDest != nil {
		e.addMuxed(source, EffectAccountInflationDestinationUpdated,
			map[string]interface{}{
				"inflation_destination": op.InflationDest.Address(),
			},
		)
	}
	changes, err := e.operation.Transaction.GetOperationChanges(e.operation.Index)
	if err != nil {
		return err
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
		return err
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
			if err := operations.AddLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); err != nil {
				return err
			}
		} else {
			operations.AddAssetDetails(details, op.Line.ToAsset(), "")
		}

		e.addMuxed(source, effect, details)
		break
	}

	return nil
}

func (e *effectsWrapper) addAllowTrustEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustAllowTrustOp()
	asset := op.Asset.ToAsset(source.ToAccountId())
	details := map[string]interface{}{
		"trustor": op.Trustor.Address(),
	}
	operations.AddAssetDetails(details, asset, "")

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
	return e.addLiquidityPoolRevokedEffect()
}

func (e *effectsWrapper) addAccountMergeEffects() {
	source := e.operation.SourceAccount()

	dest := e.operation.Operation.Body.MustDestination()
	result := e.operation.OperationResult().MustAccountMergeResult()
	details := map[string]interface{}{
		"amount":     amount.String(result.MustSourceAccountBalance()),
		"asset_type": "native",
	}

	e.addMuxed(source, EffectAccountDebited, details)
	e.addMuxed(&dest, EffectAccountCredited, details)
	e.addMuxed(source, EffectAccountRemoved, map[string]interface{}{})
}

func (e *effectsWrapper) addInflationEffects() {
	payouts := e.operation.OperationResult().MustInflationResult().MustPayouts()
	for _, payout := range payouts {
		e.addUnmuxed(&payout.Destination, EffectAccountCredited,
			map[string]interface{}{
				"amount":     amount.String(payout.Amount),
				"asset_type": "native",
			},
		)
	}
}

func (e *effectsWrapper) addManageDataEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustManageDataOp()
	details := map[string]interface{}{"name": op.DataName}
	effect := EffectType(0)
	changes, err := e.operation.Transaction.GetOperationChanges(e.operation.Index)
	if err != nil {
		return err
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

func (e *effectsWrapper) addBumpSequenceEffects() error {
	source := e.operation.SourceAccount()
	changes, err := e.operation.Transaction.GetOperationChanges(e.operation.Index)
	if err != nil {
		return err
	}

	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		before := change.Pre
		after := change.Post

		beforeAccount := before.Data.MustAccount()
		afterAccount := after.Data.MustAccount()

		if beforeAccount.SeqNum != afterAccount.SeqNum {
			details := map[string]interface{}{"new_seq": afterAccount.SeqNum}
			e.addMuxed(source, EffectSequenceBumped, details)
		}
		break
	}

	return nil
}

func setClaimableBalanceFlagDetails(details map[string]interface{}, flags xdr.ClaimableBalanceFlags) {
	if flags.IsClawbackEnabled() {
		details["claimable_balance_clawback_enabled_flag"] = true
		return
	}
}

func (e *effectsWrapper) addCreateClaimableBalanceEffects(changes []ingest.Change) error {
	source := e.operation.SourceAccount()
	var cb *xdr.ClaimableBalanceEntry
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeClaimableBalance || change.Post == nil {
			continue
		}
		cb = change.Post.Data.ClaimableBalance
		e.addClaimableBalanceEntryCreatedEffects(source, cb)
		break
	}
	if cb == nil {
		return errors.New("claimable balance entry not found")
	}

	details := map[string]interface{}{
		"amount": amount.String(cb.Amount),
	}
	operations.AddAssetDetails(details, cb.Asset, "")
	e.addMuxed(
		source,
		EffectAccountDebited,
		details,
	)

	return nil
}

func (e *effectsWrapper) addClaimableBalanceEntryCreatedEffects(source *xdr.MuxedAccount, cb *xdr.ClaimableBalanceEntry) error {
	id, err := xdr.MarshalHex(cb.BalanceId)
	if err != nil {
		return err
	}
	details := map[string]interface{}{
		"balance_id": id,
		"amount":     amount.String(cb.Amount),
		"asset":      cb.Asset.StringCanonical(),
	}
	setClaimableBalanceFlagDetails(details, cb.Flags())
	e.addMuxed(
		source,
		EffectClaimableBalanceCreated,
		details,
	)
	// EffectClaimableBalanceClaimantCreated can be generated by
	// `create_claimable_balance` operation but also by `liquidity_pool_withdraw`
	// operation causing a revocation.
	// In case of `create_claimable_balance` we use `op.Claimants` to make
	// effects backward compatible. The reason for this is that Stellar-Core
	// changes all `rel_before` predicated to `abs_before` when tx is included
	// in the ledger.
	var claimants []xdr.Claimant
	if op, ok := e.operation.Operation.Body.GetCreateClaimableBalanceOp(); ok {
		claimants = op.Claimants
	} else {
		claimants = cb.Claimants
	}
	for _, c := range claimants {
		cv0 := c.MustV0()
		e.addUnmuxed(
			&cv0.Destination,
			EffectClaimableBalanceClaimantCreated,
			map[string]interface{}{
				"balance_id": id,
				"amount":     amount.String(cb.Amount),
				"predicate":  cv0.Predicate,
				"asset":      cb.Asset.StringCanonical(),
			},
		)
	}
	return err
}

func (e *effectsWrapper) addClaimClaimableBalanceEffects(changes []ingest.Change) error {
	op := e.operation.Operation.Body.MustClaimClaimableBalanceOp()

	balanceID, err := xdr.MarshalHex(op.BalanceId)
	if err != nil {
		return fmt.Errorf("invalid balanceId in op: %d", e.operation.Index)
	}

	var cBalance xdr.ClaimableBalanceEntry
	found := false
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeClaimableBalance {
			continue
		}

		if change.Pre != nil && change.Post == nil {
			cBalance = change.Pre.Data.MustClaimableBalance()
			preBalanceID, err := xdr.MarshalHex(cBalance.BalanceId)
			if err != nil {
				return fmt.Errorf("invalid balanceId in meta changes for op: %d", e.operation.Index)
			}

			if preBalanceID == balanceID {
				found = true
				break
			}
		}
	}

	if !found {
		return fmt.Errorf("change not found for balanceId : %s", balanceID)
	}

	details := map[string]interface{}{
		"amount":     amount.String(cBalance.Amount),
		"balance_id": balanceID,
		"asset":      cBalance.Asset.StringCanonical(),
	}
	setClaimableBalanceFlagDetails(details, cBalance.Flags())
	source := e.operation.SourceAccount()
	e.addMuxed(
		source,
		EffectClaimableBalanceClaimed,
		details,
	)

	details = map[string]interface{}{
		"amount": amount.String(cBalance.Amount),
	}
	operations.AddAssetDetails(details, cBalance.Asset, "")
	e.addMuxed(
		source,
		EffectAccountCredited,
		details,
	)

	return nil
}

func (e *effectsWrapper) addIngestTradeEffects(buyer xdr.MuxedAccount, claims []xdr.ClaimAtom, isPathPayment bool) error {
	for _, claim := range claims {
		if claim.AmountSold() == 0 && claim.AmountBought() == 0 {
			continue
		}
		switch claim.Type {
		case xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool:
			if err := e.addClaimLiquidityPoolTradeEffect(claim); err != nil {
				return err
			}
		default:
			e.addClaimTradeEffects(buyer, claim, isPathPayment)
		}
	}
	return nil
}

func (e *effectsWrapper) addClaimTradeEffects(buyer xdr.MuxedAccount, claim xdr.ClaimAtom, isPathPayment bool) {
	seller := claim.SellerId()
	bd, sd := tradeDetails(buyer, seller, claim)

	tradeEffects := []EffectType{
		EffectTrade,
		EffectOfferUpdated,
		EffectOfferRemoved,
		EffectOfferCreated,
	}

	for n, effect := range tradeEffects {
		// skip EffectOfferCreated if OperationType is path_payment
		if n == 3 && isPathPayment {
			continue
		}

		e.addMuxed(
			&buyer,
			effect,
			bd,
		)

		e.addUnmuxed(
			&seller,
			effect,
			sd,
		)
	}
}

func (e *effectsWrapper) addClaimLiquidityPoolTradeEffect(claim xdr.ClaimAtom) error {
	lp, _, err := e.operation.GetLiquidityPoolAndProductDelta(&claim.LiquidityPool.LiquidityPoolId)
	if err != nil {
		return err
	}
	details := map[string]interface{}{
		"liquidity_pool": liquidityPoolDetails(lp),
		"sold": map[string]string{
			"asset":  claim.LiquidityPool.AssetSold.StringCanonical(),
			"amount": amount.String(claim.LiquidityPool.AmountSold),
		},
		"bought": map[string]string{
			"asset":  claim.LiquidityPool.AssetBought.StringCanonical(),
			"amount": amount.String(claim.LiquidityPool.AmountBought),
		},
	}
	e.addMuxed(e.operation.SourceAccount(), EffectLiquidityPoolTrade, details)
	return nil
}

func (e *effectsWrapper) addClawbackEffects() error {
	op := e.operation.Operation.Body.MustClawbackOp()
	details := map[string]interface{}{
		"amount": amount.String(op.Amount),
	}
	source := e.operation.SourceAccount()
	operations.AddAssetDetails(details, op.Asset, "")

	// The funds will be burned, but even with that, we generated an account credited effect
	e.addMuxed(
		source,
		EffectAccountCredited,
		details,
	)

	e.addMuxed(
		&op.From,
		EffectAccountDebited,
		details,
	)

	return nil
}

func (e *effectsWrapper) addClawbackClaimableBalanceEffects(changes []ingest.Change) error {
	op := e.operation.Operation.Body.MustClawbackClaimableBalanceOp()
	balanceId, err := xdr.MarshalHex(op.BalanceId)
	if err != nil {
		return errors.Wrapf(err, "Invalid balanceId in op %d", e.operation.Index)
	}
	details := map[string]interface{}{
		"balance_id": balanceId,
	}
	source := e.operation.SourceAccount()
	e.addMuxed(
		source,
		EffectClaimableBalanceClawedBack,
		details,
	)

	// Generate the account credited effect (although the funds will be burned) for the asset issuer
	for _, c := range changes {
		if c.Type == xdr.LedgerEntryTypeClaimableBalance && c.Post == nil && c.Pre != nil {
			cb := c.Pre.Data.ClaimableBalance
			details = map[string]interface{}{"amount": amount.String(cb.Amount)}
			operations.AddAssetDetails(details, cb.Asset, "")
			e.addMuxed(
				source,
				EffectAccountCredited,
				details,
			)
			break
		}
	}

	return nil
}

func (e *effectsWrapper) addSetTrustLineFlagsEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.Operation.Body.MustSetTrustLineFlagsOp()
	e.addTrustLineFlagsEffect(source, &op.Trustor, op.Asset, &op.SetFlags, &op.ClearFlags)
	return e.addLiquidityPoolRevokedEffect()
}

func (e *effectsWrapper) addTrustLineFlagsEffect(
	account *xdr.MuxedAccount,
	trustor *xdr.AccountId,
	asset xdr.Asset,
	setFlags *xdr.Uint32,
	clearFlags *xdr.Uint32) {
	details := map[string]interface{}{
		"trustor": trustor.Address(),
	}
	operations.AddAssetDetails(details, asset, "")

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

type sortableClaimableBalanceEntries []*xdr.ClaimableBalanceEntry

func (s sortableClaimableBalanceEntries) Len() int           { return len(s) }
func (s sortableClaimableBalanceEntries) Less(i, j int) bool { return s[i].Asset.LessThan(s[j].Asset) }
func (s sortableClaimableBalanceEntries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (e *effectsWrapper) addLiquidityPoolRevokedEffect() error {
	source := e.operation.SourceAccount()
	lp, delta, err := e.operation.GetLiquidityPoolAndProductDelta(nil)
	if err != nil {
		if err == operations.ErrLiquidityPoolChangeNotFound {
			// no revocation happened
			return nil
		}
		return err
	}
	changes, err := e.operation.Transaction.GetOperationChanges(e.operation.Index)
	if err != nil {
		return err
	}
	assetToCBID := map[string]string{}
	var cbs sortableClaimableBalanceEntries
	for _, change := range changes {
		if change.Type == xdr.LedgerEntryTypeClaimableBalance && change.Pre == nil && change.Post != nil {
			cb := change.Post.Data.ClaimableBalance
			id, err := xdr.MarshalHex(cb.BalanceId)
			if err != nil {
				return err
			}
			assetToCBID[cb.Asset.StringCanonical()] = id
			cbs = append(cbs, cb)
		}
	}
	if len(assetToCBID) == 0 {
		// no claimable balances were created, and thus, no revocation happened
		return nil
	}
	// Core's claimable balance metadata isn't ordered, so we order it ourselves
	// so that effects are ordered consistently
	sort.Sort(cbs)
	for _, cb := range cbs {
		if err := e.addClaimableBalanceEntryCreatedEffects(source, cb); err != nil {
			return err
		}
	}

	reservesRevoked := make([]map[string]string, 0, 2)
	for _, aa := range []base.AssetAmount{
		{
			Asset:  lp.Body.ConstantProduct.Params.AssetA.StringCanonical(),
			Amount: amount.String(-delta.ReserveA),
		},
		{
			Asset:  lp.Body.ConstantProduct.Params.AssetB.StringCanonical(),
			Amount: amount.String(-delta.ReserveB),
		},
	} {
		if cbID, ok := assetToCBID[aa.Asset]; ok {
			assetAmountDetail := map[string]string{
				"asset":                aa.Asset,
				"amount":               aa.Amount,
				"claimable_balance_id": cbID,
			}
			reservesRevoked = append(reservesRevoked, assetAmountDetail)
		}
	}
	details := map[string]interface{}{
		"liquidity_pool":   liquidityPoolDetails(lp),
		"reserves_revoked": reservesRevoked,
		"shares_revoked":   amount.String(-delta.TotalPoolShares),
	}
	e.addMuxed(source, EffectLiquidityPoolRevoked, details)
	return nil
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

func tradeDetails(buyer xdr.MuxedAccount, seller xdr.AccountId, claim xdr.ClaimAtom) (bd map[string]interface{}, sd map[string]interface{}) {
	bd = map[string]interface{}{
		"offer_id":      claim.OfferId(),
		"seller":        seller.Address(),
		"bought_amount": amount.String(claim.AmountSold()),
		"sold_amount":   amount.String(claim.AmountBought()),
	}
	operations.AddAssetDetails(bd, claim.AssetSold(), "bought_")
	operations.AddAssetDetails(bd, claim.AssetBought(), "sold_")

	sd = map[string]interface{}{
		"offer_id":      claim.OfferId(),
		"bought_amount": amount.String(claim.AmountBought()),
		"sold_amount":   amount.String(claim.AmountSold()),
	}
	operations.AddAccountAndMuxedAccountDetails(sd, buyer, "seller")
	operations.AddAssetDetails(sd, claim.AssetBought(), "bought_")
	operations.AddAssetDetails(sd, claim.AssetSold(), "sold_")

	return
}

func liquidityPoolDetails(lp *xdr.LiquidityPoolEntry) map[string]interface{} {
	return map[string]interface{}{
		"id":               operations.PoolIDToString(lp.LiquidityPoolId),
		"fee_bp":           uint32(lp.Body.ConstantProduct.Params.Fee),
		"type":             "constant_product",
		"total_trustlines": strconv.FormatInt(int64(lp.Body.ConstantProduct.PoolSharesTrustLineCount), 10),
		"total_shares":     amount.String(lp.Body.ConstantProduct.TotalPoolShares),
		"reserves": []base.AssetAmount{
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetA.StringCanonical(),
				Amount: amount.String(lp.Body.ConstantProduct.ReserveA),
			},
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetB.StringCanonical(),
				Amount: amount.String(lp.Body.ConstantProduct.ReserveB),
			},
		},
	}
}

func (e *effectsWrapper) addLiquidityPoolDepositEffect() error {
	op := e.operation.Operation.Body.MustLiquidityPoolDepositOp()
	lp, delta, err := e.operation.GetLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
	if err != nil {
		return err
	}
	details := map[string]interface{}{
		"liquidity_pool": liquidityPoolDetails(lp),
		"reserves_deposited": []base.AssetAmount{
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetA.StringCanonical(),
				Amount: amount.String(delta.ReserveA),
			},
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetB.StringCanonical(),
				Amount: amount.String(delta.ReserveB),
			},
		},
		"shares_received": amount.String(delta.TotalPoolShares),
	}
	e.addMuxed(e.operation.SourceAccount(), EffectLiquidityPoolDeposited, details)
	return nil
}

func (e *effectsWrapper) addLiquidityPoolWithdrawEffect() error {
	op := e.operation.Operation.Body.MustLiquidityPoolWithdrawOp()
	lp, delta, err := e.operation.GetLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
	if err != nil {
		return err
	}
	details := map[string]interface{}{
		"liquidity_pool": liquidityPoolDetails(lp),
		"reserves_received": []base.AssetAmount{
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetA.StringCanonical(),
				Amount: amount.String(-delta.ReserveA),
			},
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetB.StringCanonical(),
				Amount: amount.String(-delta.ReserveB),
			},
		},
		"shares_redeemed": amount.String(-delta.TotalPoolShares),
	}
	e.addMuxed(e.operation.SourceAccount(), EffectLiquidityPoolWithdrew, details)
	return nil
}

// addInvokeHostFunctionEffects iterates through the events and generates
// account_credited and account_debited effects when it sees events related to
// the Stellar Asset Contract corresponding to those effects.
func (e *effectsWrapper) addInvokeHostFunctionEffects(events []contractevents.Event) error {
	if e.operation.Network == "" {
		return errors.New("invokeHostFunction effects cannot be determined unless network passphrase is set")
	}

	source := e.operation.SourceAccount()
	for _, event := range events {
		evt, err := contractevents.NewStellarAssetContractEvent(&event, e.operation.Network)
		if err != nil {
			continue // irrelevant or unsupported event
		}

		details := make(map[string]interface{}, 4)
		operations.AddAssetDetails(details, evt.GetAsset(), "")

		//
		// Note: We ignore effects that involve contracts (until the day we have
		// contract_debited/credited effects, may it never come :pray:)
		//

		switch evt.GetType() {
		// Transfer events generate an `account_debited` effect for the `from`
		// (sender) and an `account_credited` effect for the `to` (recipient).
		case contractevents.EventTypeTransfer:
			details["contract_event_type"] = "transfer"
			transferEvent := evt.(*contractevents.TransferEvent)
			details["amount"] = amount.String128(transferEvent.Amount)
			toDetails := map[string]interface{}{}
			for key, val := range details {
				toDetails[key] = val
			}

			if strkey.IsValidEd25519PublicKey(transferEvent.From) {
				e.add(
					transferEvent.From,
					null.String{},
					EffectAccountDebited,
					details,
				)
			} else {
				details["contract"] = transferEvent.From
				e.addMuxed(source, EffectContractDebited, details)
			}

			if strkey.IsValidEd25519PublicKey(transferEvent.To) {
				e.add(
					transferEvent.To,
					null.String{},
					EffectAccountCredited,
					toDetails,
				)
			} else {
				toDetails["contract"] = transferEvent.To
				e.addMuxed(source, EffectContractCredited, toDetails)
			}

		// Mint events imply a non-native asset, and it results in a credit to
		// the `to` recipient.
		case contractevents.EventTypeMint:
			details["contract_event_type"] = "mint"
			mintEvent := evt.(*contractevents.MintEvent)
			details["amount"] = amount.String128(mintEvent.Amount)
			if strkey.IsValidEd25519PublicKey(mintEvent.To) {
				e.add(
					mintEvent.To,
					null.String{},
					EffectAccountCredited,
					details,
				)
			} else {
				details["contract"] = mintEvent.To
				e.addMuxed(source, EffectContractCredited, details)
			}

		// Clawback events result in a debit to the `from` address, but acts
		// like a burn to the recipient, so these are functionally equivalent
		case contractevents.EventTypeClawback:
			details["contract_event_type"] = "clawback"
			cbEvent := evt.(*contractevents.ClawbackEvent)
			details["amount"] = amount.String128(cbEvent.Amount)
			if strkey.IsValidEd25519PublicKey(cbEvent.From) {
				e.add(
					cbEvent.From,
					null.String{},
					EffectAccountDebited,
					details,
				)
			} else {
				details["contract"] = cbEvent.From
				e.addMuxed(source, EffectContractDebited, details)
			}

		case contractevents.EventTypeBurn:
			details["contract_event_type"] = "burn"
			burnEvent := evt.(*contractevents.BurnEvent)
			details["amount"] = amount.String128(burnEvent.Amount)
			if strkey.IsValidEd25519PublicKey(burnEvent.From) {
				e.add(
					burnEvent.From,
					null.String{},
					EffectAccountDebited,
					details,
				)
			} else {
				details["contract"] = burnEvent.From
				e.addMuxed(source, EffectContractDebited, details)
			}
		}
	}

	return nil
}

func (e *effectsWrapper) addExtendFootprintTtlEffect() error {
	op := e.operation.Operation.Body.MustExtendFootprintTtlOp()

	// Figure out which entries were affected
	changes, err := e.operation.Transaction.GetOperationChanges(e.operation.Index)
	if err != nil {
		return err
	}
	entries := make([]string, 0, len(changes))
	for _, change := range changes {
		// They should all have a post
		if change.Post == nil {
			return fmt.Errorf("invalid bump footprint expiration operation: %v", op)
		}
		var key xdr.LedgerKey
		switch change.Post.Data.Type {
		case xdr.LedgerEntryTypeTtl:
			v := change.Post.Data.MustTtl()
			if err := key.SetTtl(v.KeyHash); err != nil {
				return err
			}
		default:
			// Ignore any non-contract entries, as they couldn't have been affected.
			//
			// Should we error here? No, because there might be other entries
			// affected, for example, the user's balance.
			continue
		}
		b64, err := xdr.MarshalBase64(key)
		if err != nil {
			return err
		}
		entries = append(entries, b64)
	}
	details := map[string]interface{}{
		"entries":   entries,
		"extend_to": op.ExtendTo,
	}
	e.addMuxed(e.operation.SourceAccount(), EffectExtendFootprintTtl, details)
	return nil
}

func (e *effectsWrapper) addRestoreFootprintExpirationEffect() error {
	op := e.operation.Operation.Body.MustRestoreFootprintOp()

	// Figure out which entries were affected
	changes, err := e.operation.Transaction.GetOperationChanges(e.operation.Index)
	if err != nil {
		return err
	}
	entries := make([]string, 0, len(changes))
	for _, change := range changes {
		// They should all have a post
		if change.Post == nil {
			return fmt.Errorf("invalid restore footprint operation: %v", op)
		}
		var key xdr.LedgerKey
		switch change.Post.Data.Type {
		case xdr.LedgerEntryTypeTtl:
			v := change.Post.Data.MustTtl()
			if err := key.SetTtl(v.KeyHash); err != nil {
				return err
			}
		default:
			// Ignore any non-contract entries, as they couldn't have been affected.
			//
			// Should we error here? No, because there might be other entries
			// affected, for example, the user's balance.
			continue
		}
		b64, err := xdr.MarshalBase64(key)
		if err != nil {
			return err
		}
		entries = append(entries, b64)
	}
	details := map[string]interface{}{
		"entries": entries,
	}
	e.addMuxed(e.operation.SourceAccount(), EffectRestoreFootprint, details)
	return nil
}
