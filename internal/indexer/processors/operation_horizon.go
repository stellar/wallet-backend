package processors

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/guregu/null"
	"github.com/pkg/errors"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/utils"
	"github.com/stellar/go/protocols/horizon/base"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/contractevents"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
)

// OperationOutput is a representation of an operation that aligns with the BigQuery table history_operations
type OperationOutput struct {
	SourceAccount        string                 `json:"source_account"`
	SourceAccountMuxed   string                 `json:"source_account_muxed,omitempty"`
	Type                 int32                  `json:"type"`
	TypeString           string                 `json:"type_string"`
	OperationDetails     map[string]interface{} `json:"details"` //Details is a JSON object that varies based on operation type
	TransactionID        int64                  `json:"transaction_id"`
	OperationID          int64                  `json:"id"`
	ClosedAt             time.Time              `json:"closed_at"`
	OperationResultCode  string                 `json:"operation_result_code"`
	OperationTraceCode   string                 `json:"operation_trace_code"`
	LedgerSequence       uint32                 `json:"ledger_sequence"`
	OperationDetailsJSON map[string]interface{} `json:"details_json"`
}

type liquidityPoolDelta struct {
	ReserveA        xdr.Int64
	ReserveB        xdr.Int64
	TotalPoolShares xdr.Int64
}

// TransformOperation converts an operation from the history archive ingestion system into a form suitable for BigQuery
func TransformOperation(operation xdr.Operation, operationIndex int32, transaction ingest.LedgerTransaction, ledgerSeq int32, ledgerCloseMeta xdr.LedgerCloseMeta, network string) (OperationOutput, error) {
	outputTransactionID := toid.New(ledgerSeq, int32(transaction.Index), 0).ToInt64()
	outputOperationID := toid.New(ledgerSeq, int32(transaction.Index), operationIndex+1).ToInt64() //operationIndex needs +1 increment to stay in sync with ingest package

	sourceAccount := getOperationSourceAccount(operation, transaction)
	outputSourceAccount, err := utils.GetAccountAddressFromMuxedAccount(sourceAccount)
	if err != nil {
		return OperationOutput{}, fmt.Errorf("for operation %d (ledger id=%d): %v", operationIndex, outputOperationID, err)
	}

	var outputSourceAccountMuxed null.String
	if sourceAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		var muxedAddress string
		muxedAddress, err = sourceAccount.GetAddress()
		if err != nil {
			return OperationOutput{}, err
		}
		outputSourceAccountMuxed = null.StringFrom(muxedAddress)
	}

	outputOperationType := int32(operation.Body.Type)
	if outputOperationType < 0 {
		return OperationOutput{}, fmt.Errorf("the operation type (%d) is negative for  operation %d (operation id=%d)", outputOperationType, operationIndex, outputOperationID)
	}

	outputDetails, err := extractOperationDetails(operation, transaction, operationIndex, network)
	if err != nil {
		return OperationOutput{}, err
	}

	outputOperationTypeString, err := mapOperationType(operation)
	if err != nil {
		return OperationOutput{}, err
	}

	outputCloseTime, err := utils.GetCloseTime(ledgerCloseMeta)
	if err != nil {
		return OperationOutput{}, err
	}

	var outputOperationResultCode string
	var outputOperationTraceCode string
	outputOperationResults, ok := transaction.Result.Result.OperationResults()
	if ok {
		outputOperationResultCode = outputOperationResults[operationIndex].Code.String()
		operationResultTr, ok := outputOperationResults[operationIndex].GetTr()
		if ok {
			outputOperationTraceCode, err = mapOperationTrace(operationResultTr)
			if err != nil {
				return OperationOutput{}, err
			}
		}
	}

	outputLedgerSequence := utils.GetLedgerSequence(ledgerCloseMeta)

	transformedOperation := OperationOutput{
		SourceAccount:        outputSourceAccount,
		SourceAccountMuxed:   outputSourceAccountMuxed.String,
		Type:                 outputOperationType,
		TypeString:           outputOperationTypeString,
		TransactionID:        outputTransactionID,
		OperationID:          outputOperationID,
		OperationDetails:     outputDetails,
		ClosedAt:             outputCloseTime,
		OperationResultCode:  outputOperationResultCode,
		OperationTraceCode:   outputOperationTraceCode,
		LedgerSequence:       outputLedgerSequence,
		OperationDetailsJSON: outputDetails,
	}

	return transformedOperation, nil
}

func mapOperationType(operation xdr.Operation) (string, error) {
	var op_string_type string
	operationType := operation.Body.Type

	switch operationType {
	case xdr.OperationTypeCreateAccount:
		op_string_type = "create_account"
	case xdr.OperationTypePayment:
		op_string_type = "payment"
	case xdr.OperationTypePathPaymentStrictReceive:
		op_string_type = "path_payment_strict_receive"
	case xdr.OperationTypePathPaymentStrictSend:
		op_string_type = "path_payment_strict_send"
	case xdr.OperationTypeManageBuyOffer:
		op_string_type = "manage_buy_offer"
	case xdr.OperationTypeManageSellOffer:
		op_string_type = "manage_sell_offer"
	case xdr.OperationTypeCreatePassiveSellOffer:
		op_string_type = "create_passive_sell_offer"
	case xdr.OperationTypeSetOptions:
		op_string_type = "set_options"
	case xdr.OperationTypeChangeTrust:
		op_string_type = "change_trust"
	case xdr.OperationTypeAllowTrust:
		op_string_type = "allow_trust"
	case xdr.OperationTypeAccountMerge:
		op_string_type = "account_merge"
	case xdr.OperationTypeInflation:
		op_string_type = "inflation"
	case xdr.OperationTypeManageData:
		op_string_type = "manage_data"
	case xdr.OperationTypeBumpSequence:
		op_string_type = "bump_sequence"
	case xdr.OperationTypeCreateClaimableBalance:
		op_string_type = "create_claimable_balance"
	case xdr.OperationTypeClaimClaimableBalance:
		op_string_type = "claim_claimable_balance"
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op_string_type = "begin_sponsoring_future_reserves"
	case xdr.OperationTypeEndSponsoringFutureReserves:
		op_string_type = "end_sponsoring_future_reserves"
	case xdr.OperationTypeRevokeSponsorship:
		op_string_type = "revoke_sponsorship"
	case xdr.OperationTypeClawback:
		op_string_type = "clawback"
	case xdr.OperationTypeClawbackClaimableBalance:
		op_string_type = "clawback_claimable_balance"
	case xdr.OperationTypeSetTrustLineFlags:
		op_string_type = "set_trust_line_flags"
	case xdr.OperationTypeLiquidityPoolDeposit:
		op_string_type = "liquidity_pool_deposit"
	case xdr.OperationTypeLiquidityPoolWithdraw:
		op_string_type = "liquidity_pool_withdraw"
	case xdr.OperationTypeInvokeHostFunction:
		op_string_type = "invoke_host_function"
	case xdr.OperationTypeExtendFootprintTtl:
		op_string_type = "extend_footprint_ttl"
	case xdr.OperationTypeRestoreFootprint:
		op_string_type = "restore_footprint"
	default:
		return op_string_type, fmt.Errorf("unknown operation type: %s", operation.Body.Type.String())
	}
	return op_string_type, nil
}

func mapOperationTrace(operationTrace xdr.OperationResultTr) (string, error) {
	var operationTraceDescription string
	operationType := operationTrace.Type

	switch operationType {
	case xdr.OperationTypeCreateAccount:
		operationTraceDescription = operationTrace.CreateAccountResult.Code.String()
	case xdr.OperationTypePayment:
		operationTraceDescription = operationTrace.PaymentResult.Code.String()
	case xdr.OperationTypePathPaymentStrictReceive:
		operationTraceDescription = operationTrace.PathPaymentStrictReceiveResult.Code.String()
	case xdr.OperationTypePathPaymentStrictSend:
		operationTraceDescription = operationTrace.PathPaymentStrictSendResult.Code.String()
	case xdr.OperationTypeManageBuyOffer:
		operationTraceDescription = operationTrace.ManageBuyOfferResult.Code.String()
	case xdr.OperationTypeManageSellOffer:
		operationTraceDescription = operationTrace.ManageSellOfferResult.Code.String()
	case xdr.OperationTypeCreatePassiveSellOffer:
		operationTraceDescription = operationTrace.CreatePassiveSellOfferResult.Code.String()
	case xdr.OperationTypeSetOptions:
		operationTraceDescription = operationTrace.SetOptionsResult.Code.String()
	case xdr.OperationTypeChangeTrust:
		operationTraceDescription = operationTrace.ChangeTrustResult.Code.String()
	case xdr.OperationTypeAllowTrust:
		operationTraceDescription = operationTrace.AllowTrustResult.Code.String()
	case xdr.OperationTypeAccountMerge:
		operationTraceDescription = operationTrace.AccountMergeResult.Code.String()
	case xdr.OperationTypeInflation:
		operationTraceDescription = operationTrace.InflationResult.Code.String()
	case xdr.OperationTypeManageData:
		operationTraceDescription = operationTrace.ManageDataResult.Code.String()
	case xdr.OperationTypeBumpSequence:
		operationTraceDescription = operationTrace.BumpSeqResult.Code.String()
	case xdr.OperationTypeCreateClaimableBalance:
		operationTraceDescription = operationTrace.CreateClaimableBalanceResult.Code.String()
	case xdr.OperationTypeClaimClaimableBalance:
		operationTraceDescription = operationTrace.ClaimClaimableBalanceResult.Code.String()
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		operationTraceDescription = operationTrace.BeginSponsoringFutureReservesResult.Code.String()
	case xdr.OperationTypeEndSponsoringFutureReserves:
		operationTraceDescription = operationTrace.EndSponsoringFutureReservesResult.Code.String()
	case xdr.OperationTypeRevokeSponsorship:
		operationTraceDescription = operationTrace.RevokeSponsorshipResult.Code.String()
	case xdr.OperationTypeClawback:
		operationTraceDescription = operationTrace.ClawbackResult.Code.String()
	case xdr.OperationTypeClawbackClaimableBalance:
		operationTraceDescription = operationTrace.ClawbackClaimableBalanceResult.Code.String()
	case xdr.OperationTypeSetTrustLineFlags:
		operationTraceDescription = operationTrace.SetTrustLineFlagsResult.Code.String()
	case xdr.OperationTypeLiquidityPoolDeposit:
		operationTraceDescription = operationTrace.LiquidityPoolDepositResult.Code.String()
	case xdr.OperationTypeLiquidityPoolWithdraw:
		operationTraceDescription = operationTrace.LiquidityPoolWithdrawResult.Code.String()
	case xdr.OperationTypeInvokeHostFunction:
		operationTraceDescription = operationTrace.InvokeHostFunctionResult.Code.String()
	case xdr.OperationTypeExtendFootprintTtl:
		operationTraceDescription = operationTrace.ExtendFootprintTtlResult.Code.String()
	case xdr.OperationTypeRestoreFootprint:
		operationTraceDescription = operationTrace.RestoreFootprintResult.Code.String()
	default:
		return operationTraceDescription, fmt.Errorf("unknown operation type: %s", operationTrace.Type.String())
	}
	return operationTraceDescription, nil
}

func PoolIDToString(id xdr.PoolId) string {
	return xdr.Hash(id).HexString()
}

// operation xdr.Operation, operationIndex int32, transaction ingest.LedgerTransaction, ledgerSeq int32
func GetLiquidityPoolAndProductDelta(operationIndex int32, transaction ingest.LedgerTransaction, lpID *xdr.PoolId) (*xdr.LiquidityPoolEntry, *liquidityPoolDelta, error) {
	changes, err := transaction.GetOperationChanges(uint32(operationIndex))
	if err != nil {
		return nil, nil, err
	}

	for _, c := range changes {
		if c.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}
		// The delta can be caused by a full removal or full creation of the liquidity pool
		var lp *xdr.LiquidityPoolEntry
		var preA, preB, preShares xdr.Int64
		if c.Pre != nil {
			if lpID != nil && c.Pre.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Pre.Data.LiquidityPool
			if c.Pre.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Pre.Data.LiquidityPool.Body.Type)
			}
			cpPre := c.Pre.Data.LiquidityPool.Body.ConstantProduct
			preA, preB, preShares = cpPre.ReserveA, cpPre.ReserveB, cpPre.TotalPoolShares
		}
		var postA, postB, postShares xdr.Int64
		if c.Post != nil {
			if lpID != nil && c.Post.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Post.Data.LiquidityPool
			if c.Post.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Post.Data.LiquidityPool.Body.Type)
			}
			cpPost := c.Post.Data.LiquidityPool.Body.ConstantProduct
			postA, postB, postShares = cpPost.ReserveA, cpPost.ReserveB, cpPost.TotalPoolShares
		}
		delta := &liquidityPoolDelta{
			ReserveA:        postA - preA,
			ReserveB:        postB - preB,
			TotalPoolShares: postShares - preShares,
		}
		return lp, delta, nil
	}

	return nil, nil, fmt.Errorf("liquidity pool change not found")
}

func getOperationSourceAccount(operation xdr.Operation, transaction ingest.LedgerTransaction) xdr.MuxedAccount {
	sourceAccount := operation.SourceAccount
	if sourceAccount != nil {
		return *sourceAccount
	}

	return transaction.Envelope.SourceAccount()
}

func getSponsor(operation xdr.Operation, transaction ingest.LedgerTransaction, operationIndex int32) (*xdr.AccountId, error) {
	changes, err := transaction.GetOperationChanges(uint32(operationIndex))
	if err != nil {
		return nil, err
	}
	var signerKey string
	if setOps, ok := operation.Body.GetSetOptionsOp(); ok && setOps.Signer != nil {
		signerKey = setOps.Signer.Key.Address()
	}

	for _, c := range changes {
		// Check Signer changes
		if signerKey != "" {
			if sponsorAccount := getSignerSponsorInChange(signerKey, c); sponsorAccount != nil {
				return sponsorAccount, nil
			}
		}

		// Check Ledger key changes
		if c.Pre != nil || c.Post == nil {
			// We are only looking for entry creations denoting that a sponsor
			// is associated to the ledger entry of the operation.
			continue
		}
		if sponsorAccount := c.Post.SponsoringID(); sponsorAccount != nil {
			return sponsorAccount, nil
		}
	}

	return nil, nil
}

func getSignerSponsorInChange(signerKey string, change ingest.Change) xdr.SponsorshipDescriptor {
	if change.Type != xdr.LedgerEntryTypeAccount || change.Post == nil {
		return nil
	}

	preSigners := map[string]xdr.AccountId{}
	if change.Pre != nil {
		account := change.Pre.Data.MustAccount()
		preSigners = account.SponsorPerSigner()
	}

	account := change.Post.Data.MustAccount()
	postSigners := account.SponsorPerSigner()

	pre, preFound := preSigners[signerKey]
	post, postFound := postSigners[signerKey]

	if !postFound {
		return nil
	}

	if preFound {
		formerSponsor := pre.Address()
		newSponsor := post.Address()
		if formerSponsor == newSponsor {
			return nil
		}
	}

	return &post
}

func formatPrefix(p string) string {
	if p != "" {
		p += "_"
	}
	return p
}

func addAssetDetailsToOperationDetails(result map[string]interface{}, asset xdr.Asset, prefix string) error {
	var assetType, code, issuer string
	err := asset.Extract(&assetType, &code, &issuer)
	if err != nil {
		return err
	}

	prefix = formatPrefix(prefix)
	result[prefix+"asset_type"] = assetType

	if asset.Type == xdr.AssetTypeAssetTypeNative {
		result[prefix+"asset_id"] = int64(-5706705804583548011)
		return nil
	}

	result[prefix+"asset_code"] = code
	result[prefix+"asset_issuer"] = issuer
	result[prefix+"asset_id"] = utils.FarmHashAsset(code, issuer, assetType)

	return nil
}

func AddLiquidityPoolAssetDetails(result map[string]interface{}, lpp xdr.LiquidityPoolParameters) error {
	result["asset_type"] = "liquidity_pool_shares"
	if lpp.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
		return fmt.Errorf("unknown liquidity pool type %d", lpp.Type)
	}
	cp := lpp.ConstantProduct
	poolID, err := xdr.NewPoolId(cp.AssetA, cp.AssetB, cp.Fee)
	if err != nil {
		return err
	}
	result["liquidity_pool_id"] = PoolIDToString(poolID)
	return nil
}

func addPriceDetails(result map[string]interface{}, price xdr.Price, prefix string) error {
	prefix = formatPrefix(prefix)
	parsedPrice, err := strconv.ParseFloat(price.String(), 64)
	if err != nil {
		return err
	}
	result[prefix+"price"] = parsedPrice
	result[prefix+"price_r"] = utils.Price{
		Numerator:   int32(price.N),
		Denominator: int32(price.D),
	}
	return nil
}

func AddAccountAndMuxedAccountDetails(result map[string]interface{}, a xdr.MuxedAccount, prefix string) error {
	account_id := a.ToAccountId()
	result[prefix] = account_id.Address()
	prefix = formatPrefix(prefix)
	if a.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		muxedAccountAddress, err := a.GetAddress()
		if err != nil {
			return err
		}
		result[prefix+"muxed"] = muxedAccountAddress
		muxedAccountId, err := a.GetId()
		if err != nil {
			return err
		}
		result[prefix+"muxed_id"] = muxedAccountId
	}
	return nil
}

func addTrustLineFlagToDetails(result map[string]interface{}, f xdr.TrustLineFlags, prefix string) {
	var (
		n []int32
		s []string
	)

	if f.IsAuthorized() {
		n = append(n, int32(xdr.TrustLineFlagsAuthorizedFlag))
		s = append(s, "authorized")
	}

	if f.IsAuthorizedToMaintainLiabilitiesFlag() {
		n = append(n, int32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag))
		s = append(s, "authorized_to_maintain_liabilities")
	}

	if f.IsClawbackEnabledFlag() {
		n = append(n, int32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag))
		s = append(s, "clawback_enabled")
	}

	prefix = formatPrefix(prefix)
	result[prefix+"flags"] = n
	result[prefix+"flags_s"] = s
}

func addLedgerKeyToDetails(result map[string]interface{}, ledgerKey xdr.LedgerKey) error {
	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeAccount:
		result["account_id"] = ledgerKey.Account.AccountId.Address()
	case xdr.LedgerEntryTypeClaimableBalance:
		marshalHex, err := xdr.MarshalHex(ledgerKey.ClaimableBalance.BalanceId)
		if err != nil {
			return errors.Wrapf(err, "in claimable balance")
		}
		result["claimable_balance_id"] = marshalHex
	case xdr.LedgerEntryTypeData:
		result["data_account_id"] = ledgerKey.Data.AccountId.Address()
		result["data_name"] = string(ledgerKey.Data.DataName)
	case xdr.LedgerEntryTypeOffer:
		result["offer_id"] = int64(ledgerKey.Offer.OfferId)
	case xdr.LedgerEntryTypeTrustline:
		result["trustline_account_id"] = ledgerKey.TrustLine.AccountId.Address()
		if ledgerKey.TrustLine.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			result["trustline_liquidity_pool_id"] = PoolIDToString(*ledgerKey.TrustLine.Asset.LiquidityPoolId)
		} else {
			result["trustline_asset"] = ledgerKey.TrustLine.Asset.ToAsset().StringCanonical()
		}
	case xdr.LedgerEntryTypeLiquidityPool:
		result["liquidity_pool_id"] = PoolIDToString(ledgerKey.LiquidityPool.LiquidityPoolId)
	}
	return nil
}

func transformPath(initialPath []xdr.Asset) []utils.Path {
	if len(initialPath) == 0 {
		return nil
	}
	var path = make([]utils.Path, 0)
	for _, pathAsset := range initialPath {
		var assetType, code, issuer string
		err := pathAsset.Extract(&assetType, &code, &issuer)
		if err != nil {
			return nil
		}

		path = append(path, utils.Path{
			AssetType:   assetType,
			AssetIssuer: issuer,
			AssetCode:   code,
		})
	}
	return path
}

func findInitatingBeginSponsoringOp(operation xdr.Operation, operationIndex int32, transaction ingest.LedgerTransaction) *utils.SponsorshipOutput {
	if !transaction.Result.Successful() {
		// Failed transactions may not have a compliant sandwich structure
		// we can rely on (e.g. invalid nesting or a being operation with the wrong sponsoree ID)
		// and thus we bail out since we could return incorrect information.
		return nil
	}
	sponsoree := getOperationSourceAccount(operation, transaction).ToAccountId()
	operations := transaction.Envelope.Operations()
	for i := int(operationIndex) - 1; i >= 0; i-- {
		if beginOp, ok := operations[i].Body.GetBeginSponsoringFutureReservesOp(); ok &&
			beginOp.SponsoredId.Address() == sponsoree.Address() {
			result := utils.SponsorshipOutput{
				Operation:      operations[i],
				OperationIndex: uint32(i),
			}
			return &result
		}
	}
	return nil
}

func addOperationFlagToOperationDetails(result map[string]interface{}, flag uint32, prefix string) {
	intFlags := make([]int32, 0)
	stringFlags := make([]string, 0)

	if (int64(flag) & int64(xdr.AccountFlagsAuthRequiredFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthRequiredFlag))
		stringFlags = append(stringFlags, "auth_required")
	}

	if (int64(flag) & int64(xdr.AccountFlagsAuthRevocableFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthRevocableFlag))
		stringFlags = append(stringFlags, "auth_revocable")
	}

	if (int64(flag) & int64(xdr.AccountFlagsAuthImmutableFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthImmutableFlag))
		stringFlags = append(stringFlags, "auth_immutable")
	}

	if (int64(flag) & int64(xdr.AccountFlagsAuthClawbackEnabledFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthClawbackEnabledFlag))
		stringFlags = append(stringFlags, "auth_clawback_enabled")
	}

	prefix = formatPrefix(prefix)
	result[prefix+"flags"] = intFlags
	result[prefix+"flags_s"] = stringFlags
}

func extractOperationDetails(operation xdr.Operation, transaction ingest.LedgerTransaction, operationIndex int32, network string) (map[string]interface{}, error) {
	details := map[string]interface{}{}
	sourceAccount := getOperationSourceAccount(operation, transaction)
	operationType := operation.Body.Type

	switch operationType {
	case xdr.OperationTypeCreateAccount:
		op, ok := operation.Body.GetCreateAccountOp()
		if !ok {
			return details, fmt.Errorf("could not access CreateAccount info for this operation (index %d)", operationIndex)
		}

		if err := AddAccountAndMuxedAccountDetails(details, sourceAccount, "funder"); err != nil {
			return details, err
		}
		details["account"] = op.Destination.Address()
		details["starting_balance"] = utils.ConvertStroopValueToReal(op.StartingBalance)

	case xdr.OperationTypePayment:
		op, ok := operation.Body.GetPaymentOp()
		if !ok {
			return details, fmt.Errorf("could not access Payment info for this operation (index %d)", operationIndex)
		}

		if err := AddAccountAndMuxedAccountDetails(details, sourceAccount, "from"); err != nil {
			return details, err
		}
		if err := AddAccountAndMuxedAccountDetails(details, op.Destination, "to"); err != nil {
			return details, err
		}
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		if err := addAssetDetailsToOperationDetails(details, op.Asset, ""); err != nil {
			return details, err
		}

	case xdr.OperationTypePathPaymentStrictReceive:
		op, ok := operation.Body.GetPathPaymentStrictReceiveOp()
		if !ok {
			return details, fmt.Errorf("could not access PathPaymentStrictReceive info for this operation (index %d)", operationIndex)
		}

		if err := AddAccountAndMuxedAccountDetails(details, sourceAccount, "from"); err != nil {
			return details, err
		}
		if err := AddAccountAndMuxedAccountDetails(details, op.Destination, "to"); err != nil {
			return details, err
		}
		details["amount"] = utils.ConvertStroopValueToReal(op.DestAmount)
		details["source_amount"] = amount.String(0)
		details["source_max"] = utils.ConvertStroopValueToReal(op.SendMax)
		if err := addAssetDetailsToOperationDetails(details, op.DestAsset, ""); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.SendAsset, "source"); err != nil {
			return details, err
		}

		if transaction.Result.Successful() {
			allOperationResults, ok := transaction.Result.OperationResults()
			if !ok {
				return details, fmt.Errorf("could not access any results for this transaction")
			}
			currentOperationResult := allOperationResults[operationIndex]
			resultBody, ok := currentOperationResult.GetTr()
			if !ok {
				return details, fmt.Errorf("could not access result body for this operation (index %d)", operationIndex)
			}
			result, ok := resultBody.GetPathPaymentStrictReceiveResult()
			if !ok {
				return details, fmt.Errorf("could not access PathPaymentStrictReceive result info for this operation (index %d)", operationIndex)
			}
			details["source_amount"] = utils.ConvertStroopValueToReal(result.SendAmount())
		}

		details["path"] = transformPath(op.Path)

	case xdr.OperationTypePathPaymentStrictSend:
		op, ok := operation.Body.GetPathPaymentStrictSendOp()
		if !ok {
			return details, fmt.Errorf("could not access PathPaymentStrictSend info for this operation (index %d)", operationIndex)
		}

		if err := AddAccountAndMuxedAccountDetails(details, sourceAccount, "from"); err != nil {
			return details, err
		}
		if err := AddAccountAndMuxedAccountDetails(details, op.Destination, "to"); err != nil {
			return details, err
		}
		details["amount"] = amount.String(0)
		details["source_amount"] = utils.ConvertStroopValueToReal(op.SendAmount)
		details["destination_min"] = amount.String(op.DestMin)
		if err := addAssetDetailsToOperationDetails(details, op.DestAsset, ""); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.SendAsset, "source"); err != nil {
			return details, err
		}

		if transaction.Result.Successful() {
			allOperationResults, ok := transaction.Result.OperationResults()
			if !ok {
				return details, fmt.Errorf("could not access any results for this transaction")
			}
			currentOperationResult := allOperationResults[operationIndex]
			resultBody, ok := currentOperationResult.GetTr()
			if !ok {
				return details, fmt.Errorf("could not access result body for this operation (index %d)", operationIndex)
			}
			result, ok := resultBody.GetPathPaymentStrictSendResult()
			if !ok {
				return details, fmt.Errorf("could not access GetPathPaymentStrictSendResult result info for this operation (index %d)", operationIndex)
			}
			details["amount"] = utils.ConvertStroopValueToReal(result.DestAmount())
		}

		details["path"] = transformPath(op.Path)

	case xdr.OperationTypeManageBuyOffer:
		op, ok := operation.Body.GetManageBuyOfferOp()
		if !ok {
			return details, fmt.Errorf("could not access ManageBuyOffer info for this operation (index %d)", operationIndex)
		}

		details["offer_id"] = int64(op.OfferId)
		details["amount"] = utils.ConvertStroopValueToReal(op.BuyAmount)
		if err := addPriceDetails(details, op.Price, ""); err != nil {
			return details, err
		}

		if err := addAssetDetailsToOperationDetails(details, op.Buying, "buying"); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.Selling, "selling"); err != nil {
			return details, err
		}

	case xdr.OperationTypeManageSellOffer:
		op, ok := operation.Body.GetManageSellOfferOp()
		if !ok {
			return details, fmt.Errorf("could not access ManageSellOffer info for this operation (index %d)", operationIndex)
		}

		details["offer_id"] = int64(op.OfferId)
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		if err := addPriceDetails(details, op.Price, ""); err != nil {
			return details, err
		}

		if err := addAssetDetailsToOperationDetails(details, op.Buying, "buying"); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.Selling, "selling"); err != nil {
			return details, err
		}

	case xdr.OperationTypeCreatePassiveSellOffer:
		op, ok := operation.Body.GetCreatePassiveSellOfferOp()
		if !ok {
			return details, fmt.Errorf("could not access CreatePassiveSellOffer info for this operation (index %d)", operationIndex)
		}

		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		if err := addPriceDetails(details, op.Price, ""); err != nil {
			return details, err
		}

		if err := addAssetDetailsToOperationDetails(details, op.Buying, "buying"); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.Selling, "selling"); err != nil {
			return details, err
		}

	case xdr.OperationTypeSetOptions:
		op, ok := operation.Body.GetSetOptionsOp()
		if !ok {
			return details, fmt.Errorf("could not access GetSetOptions info for this operation (index %d)", operationIndex)
		}

		if op.InflationDest != nil {
			details["inflation_dest"] = op.InflationDest.Address()
		}

		if op.SetFlags != nil && *op.SetFlags > 0 {
			addOperationFlagToOperationDetails(details, uint32(*op.SetFlags), "set")
		}

		if op.ClearFlags != nil && *op.ClearFlags > 0 {
			addOperationFlagToOperationDetails(details, uint32(*op.ClearFlags), "clear")
		}

		if op.MasterWeight != nil {
			details["master_key_weight"] = uint32(*op.MasterWeight)
		}

		if op.LowThreshold != nil {
			details["low_threshold"] = uint32(*op.LowThreshold)
		}

		if op.MedThreshold != nil {
			details["med_threshold"] = uint32(*op.MedThreshold)
		}

		if op.HighThreshold != nil {
			details["high_threshold"] = uint32(*op.HighThreshold)
		}

		if op.HomeDomain != nil {
			details["home_domain"] = string(*op.HomeDomain)
		}

		if op.Signer != nil {
			details["signer_key"] = op.Signer.Key.Address()
			details["signer_weight"] = uint32(op.Signer.Weight)
		}

	case xdr.OperationTypeChangeTrust:
		op, ok := operation.Body.GetChangeTrustOp()
		if !ok {
			return details, fmt.Errorf("could not access GetChangeTrust info for this operation (index %d)", operationIndex)
		}

		if op.Line.Type == xdr.AssetTypeAssetTypePoolShare {
			if err := AddLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); err != nil {
				return details, err
			}
		} else {
			if err := addAssetDetailsToOperationDetails(details, op.Line.ToAsset(), ""); err != nil {
				return details, err
			}
			details["trustee"] = details["asset_issuer"]
		}

		if err := AddAccountAndMuxedAccountDetails(details, sourceAccount, "trustor"); err != nil {
			return details, err
		}
		details["limit"] = utils.ConvertStroopValueToReal(op.Limit)

	case xdr.OperationTypeAllowTrust:
		op, ok := operation.Body.GetAllowTrustOp()
		if !ok {
			return details, fmt.Errorf("could not access AllowTrust info for this operation (index %d)", operationIndex)
		}

		if err := addAssetDetailsToOperationDetails(details, op.Asset.ToAsset(sourceAccount.ToAccountId()), ""); err != nil {
			return details, err
		}
		if err := AddAccountAndMuxedAccountDetails(details, sourceAccount, "trustee"); err != nil {
			return details, err
		}
		details["trustor"] = op.Trustor.Address()
		shouldAuth := xdr.TrustLineFlags(op.Authorize).IsAuthorized()
		details["authorize"] = shouldAuth
		shouldAuthLiabilities := xdr.TrustLineFlags(op.Authorize).IsAuthorizedToMaintainLiabilitiesFlag()
		if shouldAuthLiabilities {
			details["authorize_to_maintain_liabilities"] = shouldAuthLiabilities
		}
		shouldClawbackEnabled := xdr.TrustLineFlags(op.Authorize).IsClawbackEnabledFlag()
		if shouldClawbackEnabled {
			details["clawback_enabled"] = shouldClawbackEnabled
		}

	case xdr.OperationTypeAccountMerge:
		destinationAccount, ok := operation.Body.GetDestination()
		if !ok {
			return details, fmt.Errorf("could not access Destination info for this operation (index %d)", operationIndex)
		}

		if err := AddAccountAndMuxedAccountDetails(details, sourceAccount, "account"); err != nil {
			return details, err
		}
		if err := AddAccountAndMuxedAccountDetails(details, destinationAccount, "into"); err != nil {
			return details, err
		}

	case xdr.OperationTypeInflation:
		// Inflation operations don't have information that affects the details struct
	case xdr.OperationTypeManageData:
		op, ok := operation.Body.GetManageDataOp()
		if !ok {
			return details, fmt.Errorf("could not access GetManageData info for this operation (index %d)", operationIndex)
		}

		details["name"] = string(op.DataName)
		if op.DataValue != nil {
			details["value"] = base64.StdEncoding.EncodeToString(*op.DataValue)
		} else {
			details["value"] = nil
		}

	case xdr.OperationTypeBumpSequence:
		op, ok := operation.Body.GetBumpSequenceOp()
		if !ok {
			return details, fmt.Errorf("could not access BumpSequence info for this operation (index %d)", operationIndex)
		}
		details["bump_to"] = fmt.Sprintf("%d", op.BumpTo)

	case xdr.OperationTypeClaimClaimableBalance:
		op := operation.Body.MustClaimClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			return details, fmt.Errorf("invalid balanceId in op: %d", operationIndex)
		}
		details["balance_id"] = balanceID
		if err := AddAccountAndMuxedAccountDetails(details, sourceAccount, "claimant"); err != nil {
			return details, err
		}

	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op := operation.Body.MustBeginSponsoringFutureReservesOp()
		details["sponsored_id"] = op.SponsoredId.Address()

	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorOp := findInitatingBeginSponsoringOp(operation, operationIndex, transaction)
		if beginSponsorOp != nil {
			beginSponsorshipSource := getOperationSourceAccount(beginSponsorOp.Operation, transaction)
			if err := AddAccountAndMuxedAccountDetails(details, beginSponsorshipSource, "begin_sponsor"); err != nil {
				return details, err
			}
		}

	case xdr.OperationTypeRevokeSponsorship:
		op := operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			if err := addLedgerKeyToDetails(details, *op.LedgerKey); err != nil {
				return details, err
			}
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			details["signer_account_id"] = op.Signer.AccountId.Address()
			details["signer_key"] = op.Signer.SignerKey.Address()
		}

	case xdr.OperationTypeClawback:
		op := operation.Body.MustClawbackOp()
		if err := addAssetDetailsToOperationDetails(details, op.Asset, ""); err != nil {
			return details, err
		}
		if err := AddAccountAndMuxedAccountDetails(details, op.From, "from"); err != nil {
			return details, err
		}
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)

	case xdr.OperationTypeClawbackClaimableBalance:
		op := operation.Body.MustClawbackClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			return details, fmt.Errorf("invalid balanceId in op: %d", operationIndex)
		}
		details["balance_id"] = balanceID

	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.Body.MustSetTrustLineFlagsOp()
		details["trustor"] = op.Trustor.Address()
		if err := addAssetDetailsToOperationDetails(details, op.Asset, ""); err != nil {
			return details, err
		}
		if op.SetFlags > 0 {
			addTrustLineFlagToDetails(details, xdr.TrustLineFlags(op.SetFlags), "set")

		}
		if op.ClearFlags > 0 {
			addTrustLineFlagToDetails(details, xdr.TrustLineFlags(op.ClearFlags), "clear")
		}

	case xdr.OperationTypeLiquidityPoolDeposit:
		op := operation.Body.MustLiquidityPoolDepositOp()
		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		var (
			assetA, assetB         xdr.Asset
			depositedA, depositedB xdr.Int64
			sharesReceived         xdr.Int64
		)
		if transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := GetLiquidityPoolAndProductDelta(operationIndex, transaction, &op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA, params.AssetB
			depositedA, depositedB = delta.ReserveA, delta.ReserveB
			sharesReceived = delta.TotalPoolShares
		}

		// Process ReserveA Details
		if err := addAssetDetailsToOperationDetails(details, assetA, "reserve_a"); err != nil {
			return details, err
		}
		details["reserve_a_max_amount"] = utils.ConvertStroopValueToReal(op.MaxAmountA)
		depositA, err := strconv.ParseFloat(amount.String(depositedA), 64)
		if err != nil {
			return details, err
		}
		details["reserve_a_deposit_amount"] = depositA

		//Process ReserveB Details
		if err = addAssetDetailsToOperationDetails(details, assetB, "reserve_b"); err != nil {
			return details, err
		}
		details["reserve_b_max_amount"] = utils.ConvertStroopValueToReal(op.MaxAmountB)
		depositB, err := strconv.ParseFloat(amount.String(depositedB), 64)
		if err != nil {
			return details, err
		}
		details["reserve_b_deposit_amount"] = depositB

		if err = addPriceDetails(details, op.MinPrice, "min"); err != nil {
			return details, err
		}
		if err = addPriceDetails(details, op.MaxPrice, "max"); err != nil {
			return details, err
		}

		sharesToFloat, err := strconv.ParseFloat(amount.String(sharesReceived), 64)
		if err != nil {
			return details, err
		}
		details["shares_received"] = sharesToFloat

	case xdr.OperationTypeLiquidityPoolWithdraw:
		op := operation.Body.MustLiquidityPoolWithdrawOp()
		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		var (
			assetA, assetB       xdr.Asset
			receivedA, receivedB xdr.Int64
		)
		if transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := GetLiquidityPoolAndProductDelta(operationIndex, transaction, &op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA, params.AssetB
			receivedA, receivedB = -delta.ReserveA, -delta.ReserveB
		}
		// Process AssetA Details
		if err := addAssetDetailsToOperationDetails(details, assetA, "reserve_a"); err != nil {
			return details, err
		}
		details["reserve_a_min_amount"] = utils.ConvertStroopValueToReal(op.MinAmountA)
		details["reserve_a_withdraw_amount"] = utils.ConvertStroopValueToReal(receivedA)

		// Process AssetB Details
		if err := addAssetDetailsToOperationDetails(details, assetB, "reserve_b"); err != nil {
			return details, err
		}
		details["reserve_b_min_amount"] = utils.ConvertStroopValueToReal(op.MinAmountB)
		details["reserve_b_withdraw_amount"] = utils.ConvertStroopValueToReal(receivedB)

		details["shares"] = utils.ConvertStroopValueToReal(op.Amount)

	case xdr.OperationTypeInvokeHostFunction:
		op := operation.Body.MustInvokeHostFunctionOp()
		details["function"] = op.HostFunction.Type.String()

		switch op.HostFunction.Type {
		case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
			invokeArgs := op.HostFunction.MustInvokeContract()
			args := make([]xdr.ScVal, 0, len(invokeArgs.Args)+2)
			args = append(args, xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &invokeArgs.ContractAddress})
			args = append(args, xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &invokeArgs.FunctionName})
			args = append(args, invokeArgs.Args...)

			details["type"] = "invoke_contract"

			contractId, err := invokeArgs.ContractAddress.String()
			if err != nil {
				return nil, err
			}

			transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractId
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			details["parameters"], details["parameters_decoded"] = serializeParameters(args)

			if balanceChanges, err := parseAssetBalanceChangesFromContractEvents(transaction, network); err != nil {
				return nil, err
			} else {
				details["asset_balance_changes"] = balanceChanges
			}

		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			args := op.HostFunction.MustCreateContract()
			details["type"] = "create_contract"

			transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			preimageTypeMap := switchContractIdPreimageType(args.ContractIdPreimage)
			for key, val := range preimageTypeMap {
				if _, ok := preimageTypeMap[key]; ok {
					details[key] = val
				}
			}
		case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
			details["type"] = "upload_wasm"
			transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			args := op.HostFunction.MustCreateContractV2()
			details["type"] = "create_contract_v2"

			transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			// ConstructorArgs is a list of ScVals
			// This will initially be handled the same as InvokeContractParams until a different
			// model is found necessary.
			constructorArgs := args.ConstructorArgs
			details["parameters"], details["parameters_decoded"] = serializeParameters(constructorArgs)

			preimageTypeMap := switchContractIdPreimageType(args.ContractIdPreimage)
			for key, val := range preimageTypeMap {
				if _, ok := preimageTypeMap[key]; ok {
					details[key] = val
				}
			}
		default:
			panic(fmt.Errorf("unknown host function type: %s", op.HostFunction.Type))
		}
	case xdr.OperationTypeExtendFootprintTtl:
		op := operation.Body.MustExtendFootprintTtlOp()
		details["type"] = "extend_footprint_ttl"
		details["extend_to"] = op.ExtendTo

		transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
		details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	case xdr.OperationTypeRestoreFootprint:
		details["type"] = "restore_footprint"

		transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
		details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	default:
		return details, fmt.Errorf("unknown operation type: %s", operation.Body.Type.String())
	}

	sponsor, err := getSponsor(operation, transaction, operationIndex)
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		details["sponsor"] = sponsor.Address()
	}

	return details, nil
}

// transactionOperationWrapper represents the data for a single operation within a transaction
type TransactionOperationWrapper struct {
	Index          uint32
	Transaction    ingest.LedgerTransaction
	Operation      xdr.Operation
	LedgerSequence uint32
	Network        string
	LedgerClosed   time.Time
}

// ID returns the ID for the operation.
func (operation *TransactionOperationWrapper) ID() int64 {
	return toid.New(
		int32(operation.LedgerSequence),
		int32(operation.Transaction.Index),
		int32(operation.Index+1),
	).ToInt64()
}

// Order returns the operation order.
func (operation *TransactionOperationWrapper) Order() uint32 {
	return operation.Index + 1
}

// TransactionID returns the id for the transaction related with this operation.
func (operation *TransactionOperationWrapper) TransactionID() int64 {
	return toid.New(int32(operation.LedgerSequence), int32(operation.Transaction.Index), 0).ToInt64()
}

// SourceAccount returns the operation's source account.
func (operation *TransactionOperationWrapper) SourceAccount() *xdr.MuxedAccount {
	sourceAccount := operation.Operation.SourceAccount
	if sourceAccount != nil {
		return sourceAccount
	} else {
		ret := operation.Transaction.Envelope.SourceAccount()
		return &ret
	}
}

// OperationType returns the operation type.
func (operation *TransactionOperationWrapper) OperationType() xdr.OperationType {
	return operation.Operation.Body.Type
}

func (operation *TransactionOperationWrapper) getSignerSponsorInChange(signerKey string, change ingest.Change) xdr.SponsorshipDescriptor {
	if change.Type != xdr.LedgerEntryTypeAccount || change.Post == nil {
		return nil
	}

	preSigners := map[string]xdr.AccountId{}
	if change.Pre != nil {
		account := change.Pre.Data.MustAccount()
		preSigners = account.SponsorPerSigner()
	}

	account := change.Post.Data.MustAccount()
	postSigners := account.SponsorPerSigner()

	pre, preFound := preSigners[signerKey]
	post, postFound := postSigners[signerKey]

	if !postFound {
		return nil
	}

	if preFound {
		formerSponsor := pre.Address()
		newSponsor := post.Address()
		if formerSponsor == newSponsor {
			return nil
		}
	}

	return &post
}

func (operation *TransactionOperationWrapper) getSponsor() (*xdr.AccountId, error) {
	changes, err := operation.Transaction.GetOperationChanges(operation.Index)
	if err != nil {
		return nil, err
	}
	var signerKey string
	if setOps, ok := operation.Operation.Body.GetSetOptionsOp(); ok && setOps.Signer != nil {
		signerKey = setOps.Signer.Key.Address()
	}

	for _, c := range changes {
		// Check Signer changes
		if signerKey != "" {
			if sponsorAccount := operation.getSignerSponsorInChange(signerKey, c); sponsorAccount != nil {
				return sponsorAccount, nil
			}
		}

		// Check Ledger key changes
		if c.Pre != nil || c.Post == nil {
			// We are only looking for entry creations denoting that a sponsor
			// is associated to the ledger entry of the operation.
			continue
		}
		if sponsorAccount := c.Post.SponsoringID(); sponsorAccount != nil {
			return sponsorAccount, nil
		}
	}

	return nil, nil
}

var ErrLiquidityPoolChangeNotFound = errors.New("liquidity pool change not found")

func (operation *TransactionOperationWrapper) GetLiquidityPoolAndProductDelta(lpID *xdr.PoolId) (*xdr.LiquidityPoolEntry, *liquidityPoolDelta, error) {
	changes, err := operation.Transaction.GetOperationChanges(operation.Index)
	if err != nil {
		return nil, nil, err
	}

	for _, c := range changes {
		if c.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}
		// The delta can be caused by a full removal or full creation of the liquidity pool
		var lp *xdr.LiquidityPoolEntry
		var preA, preB, preShares xdr.Int64
		if c.Pre != nil {
			if lpID != nil && c.Pre.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Pre.Data.LiquidityPool
			if c.Pre.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Pre.Data.LiquidityPool.Body.Type)
			}
			cpPre := c.Pre.Data.LiquidityPool.Body.ConstantProduct
			preA, preB, preShares = cpPre.ReserveA, cpPre.ReserveB, cpPre.TotalPoolShares
		}
		var postA, postB, postShares xdr.Int64
		if c.Post != nil {
			if lpID != nil && c.Post.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Post.Data.LiquidityPool
			if c.Post.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Post.Data.LiquidityPool.Body.Type)
			}
			cpPost := c.Post.Data.LiquidityPool.Body.ConstantProduct
			postA, postB, postShares = cpPost.ReserveA, cpPost.ReserveB, cpPost.TotalPoolShares
		}
		delta := &liquidityPoolDelta{
			ReserveA:        postA - preA,
			ReserveB:        postB - preB,
			TotalPoolShares: postShares - preShares,
		}
		return lp, delta, nil
	}

	return nil, nil, ErrLiquidityPoolChangeNotFound
}

// OperationResult returns the operation's result record
func (operation *TransactionOperationWrapper) OperationResult() *xdr.OperationResultTr {
	results, _ := operation.Transaction.Result.OperationResults()
	tr := results[operation.Index].MustTr()
	return &tr
}

func (operation *TransactionOperationWrapper) findInitatingBeginSponsoringOp() *TransactionOperationWrapper {
	if !operation.Transaction.Result.Successful() {
		// Failed transactions may not have a compliant sandwich structure
		// we can rely on (e.g. invalid nesting or a being operation with the wrong sponsoree ID)
		// and thus we bail out since we could return incorrect information.
		return nil
	}
	sponsoree := operation.SourceAccount().ToAccountId()
	operations := operation.Transaction.Envelope.Operations()
	for i := int(operation.Index) - 1; i >= 0; i-- {
		if beginOp, ok := operations[i].Body.GetBeginSponsoringFutureReservesOp(); ok &&
			beginOp.SponsoredId.Address() == sponsoree.Address() {
			result := *operation
			result.Index = uint32(i)
			result.Operation = operations[i]
			return &result
		}
	}
	return nil
}

// Details returns the operation details as a map which can be stored as JSON.
func (operation *TransactionOperationWrapper) Details() (map[string]interface{}, error) {
	details := map[string]interface{}{}
	source := operation.SourceAccount()
	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		op := operation.Operation.Body.MustCreateAccountOp()
		AddAccountAndMuxedAccountDetails(details, *source, "funder")
		details["account"] = op.Destination.Address()
		details["starting_balance"] = amount.String(op.StartingBalance)
	case xdr.OperationTypePayment:
		op := operation.Operation.Body.MustPaymentOp()
		AddAccountAndMuxedAccountDetails(details, *source, "from")
		AddAccountAndMuxedAccountDetails(details, op.Destination, "to")
		details["amount"] = amount.String(op.Amount)
		AddAssetDetails(details, op.Asset, "")
	case xdr.OperationTypePathPaymentStrictReceive:
		op := operation.Operation.Body.MustPathPaymentStrictReceiveOp()
		AddAccountAndMuxedAccountDetails(details, *source, "from")
		AddAccountAndMuxedAccountDetails(details, op.Destination, "to")

		details["amount"] = amount.String(op.DestAmount)
		details["source_amount"] = amount.String(0)
		details["source_max"] = amount.String(op.SendMax)
		AddAssetDetails(details, op.DestAsset, "")
		AddAssetDetails(details, op.SendAsset, "source_")

		if operation.Transaction.Result.Successful() {
			result := operation.OperationResult().MustPathPaymentStrictReceiveResult()
			details["source_amount"] = amount.String(result.SendAmount())
		}

		var path = make([]map[string]interface{}, len(op.Path))
		for i := range op.Path {
			path[i] = make(map[string]interface{})
			AddAssetDetails(path[i], op.Path[i], "")
		}
		details["path"] = path

	case xdr.OperationTypePathPaymentStrictSend:
		op := operation.Operation.Body.MustPathPaymentStrictSendOp()
		AddAccountAndMuxedAccountDetails(details, *source, "from")
		AddAccountAndMuxedAccountDetails(details, op.Destination, "to")

		details["amount"] = amount.String(0)
		details["source_amount"] = amount.String(op.SendAmount)
		details["destination_min"] = amount.String(op.DestMin)
		AddAssetDetails(details, op.DestAsset, "")
		AddAssetDetails(details, op.SendAsset, "source_")

		if operation.Transaction.Result.Successful() {
			result := operation.OperationResult().MustPathPaymentStrictSendResult()
			details["amount"] = amount.String(result.DestAmount())
		}

		var path = make([]map[string]interface{}, len(op.Path))
		for i := range op.Path {
			path[i] = make(map[string]interface{})
			AddAssetDetails(path[i], op.Path[i], "")
		}
		details["path"] = path
	case xdr.OperationTypeManageBuyOffer:
		op := operation.Operation.Body.MustManageBuyOfferOp()
		details["offer_id"] = op.OfferId
		details["amount"] = amount.String(op.BuyAmount)
		details["price"] = op.Price.String()
		details["price_r"] = map[string]interface{}{
			"n": op.Price.N,
			"d": op.Price.D,
		}
		AddAssetDetails(details, op.Buying, "buying_")
		AddAssetDetails(details, op.Selling, "selling_")
	case xdr.OperationTypeManageSellOffer:
		op := operation.Operation.Body.MustManageSellOfferOp()
		details["offer_id"] = op.OfferId
		details["amount"] = amount.String(op.Amount)
		details["price"] = op.Price.String()
		details["price_r"] = map[string]interface{}{
			"n": op.Price.N,
			"d": op.Price.D,
		}
		AddAssetDetails(details, op.Buying, "buying_")
		AddAssetDetails(details, op.Selling, "selling_")
	case xdr.OperationTypeCreatePassiveSellOffer:
		op := operation.Operation.Body.MustCreatePassiveSellOfferOp()
		details["amount"] = amount.String(op.Amount)
		details["price"] = op.Price.String()
		details["price_r"] = map[string]interface{}{
			"n": op.Price.N,
			"d": op.Price.D,
		}
		AddAssetDetails(details, op.Buying, "buying_")
		AddAssetDetails(details, op.Selling, "selling_")
	case xdr.OperationTypeSetOptions:
		op := operation.Operation.Body.MustSetOptionsOp()

		if op.InflationDest != nil {
			details["inflation_dest"] = op.InflationDest.Address()
		}

		if op.SetFlags != nil && *op.SetFlags > 0 {
			addAuthFlagDetails(details, xdr.AccountFlags(*op.SetFlags), "set")
		}

		if op.ClearFlags != nil && *op.ClearFlags > 0 {
			addAuthFlagDetails(details, xdr.AccountFlags(*op.ClearFlags), "clear")
		}

		if op.MasterWeight != nil {
			details["master_key_weight"] = *op.MasterWeight
		}

		if op.LowThreshold != nil {
			details["low_threshold"] = *op.LowThreshold
		}

		if op.MedThreshold != nil {
			details["med_threshold"] = *op.MedThreshold
		}

		if op.HighThreshold != nil {
			details["high_threshold"] = *op.HighThreshold
		}

		if op.HomeDomain != nil {
			details["home_domain"] = *op.HomeDomain
		}

		if op.Signer != nil {
			details["signer_key"] = op.Signer.Key.Address()
			details["signer_weight"] = op.Signer.Weight
		}
	case xdr.OperationTypeChangeTrust:
		op := operation.Operation.Body.MustChangeTrustOp()
		if op.Line.Type == xdr.AssetTypeAssetTypePoolShare {
			if err := AddLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); err != nil {
				return nil, err
			}
		} else {
			AddAssetDetails(details, op.Line.ToAsset(), "")
			details["trustee"] = details["asset_issuer"]
		}
		AddAccountAndMuxedAccountDetails(details, *source, "trustor")
		details["limit"] = amount.String(op.Limit)
	case xdr.OperationTypeAllowTrust:
		op := operation.Operation.Body.MustAllowTrustOp()
		AddAssetDetails(details, op.Asset.ToAsset(source.ToAccountId()), "")
		AddAccountAndMuxedAccountDetails(details, *source, "trustee")
		details["trustor"] = op.Trustor.Address()
		details["authorize"] = xdr.TrustLineFlags(op.Authorize).IsAuthorized()
		authLiabilities := xdr.TrustLineFlags(op.Authorize).IsAuthorizedToMaintainLiabilitiesFlag()
		if authLiabilities {
			details["authorize_to_maintain_liabilities"] = authLiabilities
		}
		clawbackEnabled := xdr.TrustLineFlags(op.Authorize).IsClawbackEnabledFlag()
		if clawbackEnabled {
			details["clawback_enabled"] = clawbackEnabled
		}
	case xdr.OperationTypeAccountMerge:
		AddAccountAndMuxedAccountDetails(details, *source, "account")
		AddAccountAndMuxedAccountDetails(details, operation.Operation.Body.MustDestination(), "into")
	case xdr.OperationTypeInflation:
		// no inflation details, presently
	case xdr.OperationTypeManageData:
		op := operation.Operation.Body.MustManageDataOp()
		details["name"] = string(op.DataName)
		if op.DataValue != nil {
			details["value"] = base64.StdEncoding.EncodeToString(*op.DataValue)
		} else {
			details["value"] = nil
		}
	case xdr.OperationTypeBumpSequence:
		op := operation.Operation.Body.MustBumpSequenceOp()
		details["bump_to"] = fmt.Sprintf("%d", op.BumpTo)
	case xdr.OperationTypeCreateClaimableBalance:
		op := operation.Operation.Body.MustCreateClaimableBalanceOp()
		details["asset"] = op.Asset.StringCanonical()
		details["amount"] = amount.String(op.Amount)
		var claimants []utils.Claimant
		for _, c := range op.Claimants {
			cv0 := c.MustV0()
			claimants = append(claimants, utils.Claimant{
				Destination: cv0.Destination.Address(),
				Predicate:   cv0.Predicate,
			})
		}
		details["claimants"] = claimants
	case xdr.OperationTypeClaimClaimableBalance:
		op := operation.Operation.Body.MustClaimClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			panic(fmt.Errorf("invalid balanceId in op: %d", operation.Index))
		}
		details["balance_id"] = balanceID
		AddAccountAndMuxedAccountDetails(details, *source, "claimant")
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op := operation.Operation.Body.MustBeginSponsoringFutureReservesOp()
		details["sponsored_id"] = op.SponsoredId.Address()
	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorshipOp := operation.findInitatingBeginSponsoringOp()
		if beginSponsorshipOp != nil {
			beginSponsorshipSource := beginSponsorshipOp.SourceAccount()
			AddAccountAndMuxedAccountDetails(details, *beginSponsorshipSource, "begin_sponsor")
		}
	case xdr.OperationTypeRevokeSponsorship:
		op := operation.Operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			if err := addLedgerKeyDetails(details, *op.LedgerKey); err != nil {
				return nil, err
			}
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			details["signer_account_id"] = op.Signer.AccountId.Address()
			details["signer_key"] = op.Signer.SignerKey.Address()
		}
	case xdr.OperationTypeClawback:
		op := operation.Operation.Body.MustClawbackOp()
		AddAssetDetails(details, op.Asset, "")
		AddAccountAndMuxedAccountDetails(details, op.From, "from")
		details["amount"] = amount.String(op.Amount)
	case xdr.OperationTypeClawbackClaimableBalance:
		op := operation.Operation.Body.MustClawbackClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			panic(fmt.Errorf("invalid balanceId in op: %d", operation.Index))
		}
		details["balance_id"] = balanceID
	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.Operation.Body.MustSetTrustLineFlagsOp()
		details["trustor"] = op.Trustor.Address()
		AddAssetDetails(details, op.Asset, "")
		if op.SetFlags > 0 {
			addTrustLineFlagDetails(details, xdr.TrustLineFlags(op.SetFlags), "set")
		}

		if op.ClearFlags > 0 {
			addTrustLineFlagDetails(details, xdr.TrustLineFlags(op.ClearFlags), "clear")
		}
	case xdr.OperationTypeLiquidityPoolDeposit:
		op := operation.Operation.Body.MustLiquidityPoolDepositOp()
		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		var (
			assetA, assetB         string
			depositedA, depositedB xdr.Int64
			sharesReceived         xdr.Int64
		)
		if operation.Transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := operation.GetLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA.StringCanonical(), params.AssetB.StringCanonical()
			depositedA, depositedB = delta.ReserveA, delta.ReserveB
			sharesReceived = delta.TotalPoolShares
		}
		details["reserves_max"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(op.MaxAmountA)},
			{Asset: assetB, Amount: amount.String(op.MaxAmountB)},
		}
		details["min_price"] = op.MinPrice.String()
		details["min_price_r"] = map[string]interface{}{
			"n": op.MinPrice.N,
			"d": op.MinPrice.D,
		}
		details["max_price"] = op.MaxPrice.String()
		details["max_price_r"] = map[string]interface{}{
			"n": op.MaxPrice.N,
			"d": op.MaxPrice.D,
		}
		details["reserves_deposited"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(depositedA)},
			{Asset: assetB, Amount: amount.String(depositedB)},
		}
		details["shares_received"] = amount.String(sharesReceived)
	case xdr.OperationTypeLiquidityPoolWithdraw:
		op := operation.Operation.Body.MustLiquidityPoolWithdrawOp()
		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		var (
			assetA, assetB       string
			receivedA, receivedB xdr.Int64
		)
		if operation.Transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := operation.GetLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA.StringCanonical(), params.AssetB.StringCanonical()
			receivedA, receivedB = -delta.ReserveA, -delta.ReserveB
		}
		details["reserves_min"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(op.MinAmountA)},
			{Asset: assetB, Amount: amount.String(op.MinAmountB)},
		}
		details["shares"] = amount.String(op.Amount)
		details["reserves_received"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(receivedA)},
			{Asset: assetB, Amount: amount.String(receivedB)},
		}
	case xdr.OperationTypeInvokeHostFunction:
		op := operation.Operation.Body.MustInvokeHostFunctionOp()
		details["function"] = op.HostFunction.Type.String()

		switch op.HostFunction.Type {
		case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
			invokeArgs := op.HostFunction.MustInvokeContract()
			args := make([]xdr.ScVal, 0, len(invokeArgs.Args)+2)
			args = append(args, xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &invokeArgs.ContractAddress})
			args = append(args, xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &invokeArgs.FunctionName})
			args = append(args, invokeArgs.Args...)

			details["type"] = "invoke_contract"

			contractId, err := invokeArgs.ContractAddress.String()
			if err != nil {
				return nil, err
			}

			transactionEnvelope := getTransactionV1Envelope(operation.Transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractId
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			details["parameters"], details["parameters_decoded"] = serializeParameters(args)

			if balanceChanges, err := operation.parseAssetBalanceChangesFromContractEvents(); err != nil {
				return nil, err
			} else {
				details["asset_balance_changes"] = balanceChanges
			}

		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			args := op.HostFunction.MustCreateContract()
			details["type"] = "create_contract"

			transactionEnvelope := getTransactionV1Envelope(operation.Transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			preimageTypeMap := switchContractIdPreimageType(args.ContractIdPreimage)
			for key, val := range preimageTypeMap {
				if _, ok := preimageTypeMap[key]; ok {
					details[key] = val
				}
			}
		case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
			details["type"] = "upload_wasm"
			transactionEnvelope := getTransactionV1Envelope(operation.Transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			args := op.HostFunction.MustCreateContractV2()
			details["type"] = "create_contract_v2"

			transactionEnvelope := getTransactionV1Envelope(operation.Transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			// ConstructorArgs is a list of ScVals
			// This will initially be handled the same as InvokeContractParams until a different
			// model is found necessary.
			constructorArgs := args.ConstructorArgs
			details["parameters"], details["parameters_decoded"] = serializeParameters(constructorArgs)

			preimageTypeMap := switchContractIdPreimageType(args.ContractIdPreimage)
			for key, val := range preimageTypeMap {
				if _, ok := preimageTypeMap[key]; ok {
					details[key] = val
				}
			}
		default:
			panic(fmt.Errorf("unknown host function type: %s", op.HostFunction.Type))
		}
	case xdr.OperationTypeExtendFootprintTtl:
		op := operation.Operation.Body.MustExtendFootprintTtlOp()
		details["type"] = "extend_footprint_ttl"
		details["extend_to"] = op.ExtendTo

		transactionEnvelope := getTransactionV1Envelope(operation.Transaction.Envelope)
		details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	case xdr.OperationTypeRestoreFootprint:
		details["type"] = "restore_footprint"

		transactionEnvelope := getTransactionV1Envelope(operation.Transaction.Envelope)
		details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	default:
		panic(fmt.Errorf("unknown operation type: %s", operation.OperationType()))
	}

	sponsor, err := operation.getSponsor()
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		details["sponsor"] = sponsor.Address()
	}

	return details, nil
}

func getTransactionV1Envelope(transactionEnvelope xdr.TransactionEnvelope) xdr.TransactionV1Envelope {
	switch transactionEnvelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		return transactionEnvelope.MustV1()
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		return transactionEnvelope.MustFeeBump().Tx.InnerTx.MustV1()
	}

	return xdr.TransactionV1Envelope{}
}

func contractIdFromTxEnvelope(transactionEnvelope xdr.TransactionV1Envelope) string {
	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		contractId := contractIdFromContractData(ledgerKey)
		if contractId != "" {
			return contractId
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly {
		contractId := contractIdFromContractData(ledgerKey)
		if contractId != "" {
			return contractId
		}
	}

	return ""
}

func contractIdFromContractData(ledgerKey xdr.LedgerKey) string {
	contractData, ok := ledgerKey.GetContractData()
	if !ok {
		return ""
	}
	contractIdHash, ok := contractData.Contract.GetContractId()
	if !ok {
		return ""
	}

	contractIdByte, _ := contractIdHash.MarshalBinary()
	contractId, _ := strkey.Encode(strkey.VersionByteContract, contractIdByte)
	return contractId
}

func contractCodeHashFromTxEnvelope(transactionEnvelope xdr.TransactionV1Envelope) string {
	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly {
		contractCode := contractCodeFromContractData(ledgerKey)
		if contractCode != "" {
			return contractCode
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		contractCode := contractCodeFromContractData(ledgerKey)
		if contractCode != "" {
			return contractCode
		}
	}

	return ""
}

func ledgerKeyHashFromTxEnvelope(transactionEnvelope xdr.TransactionV1Envelope) []string {
	var ledgerKeyHash []string
	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly {
		if utils.LedgerKeyToLedgerKeyHash(ledgerKey) != "" {
			ledgerKeyHash = append(ledgerKeyHash, utils.LedgerKeyToLedgerKeyHash(ledgerKey))
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		if utils.LedgerKeyToLedgerKeyHash(ledgerKey) != "" {
			ledgerKeyHash = append(ledgerKeyHash, utils.LedgerKeyToLedgerKeyHash(ledgerKey))
		}
	}

	return ledgerKeyHash
}

func contractCodeFromContractData(ledgerKey xdr.LedgerKey) string {
	contractCode, ok := ledgerKey.GetContractCode()
	if !ok {
		return ""
	}

	contractCodeHash := contractCode.Hash.HexString()
	return contractCodeHash
}

// Searches an operation for SAC events that are of a type which represent
// asset balances having changed.
//
// SAC events have a one-to-one association to SAC contract fn invocations.
// i.e. invoke the 'mint' function, will trigger one Mint Event to be emitted capturing the fn args.
//
// SAC events that involve asset balance changes follow some standard data formats.
// The 'amount' in the event is expressed as Int128Parts, which carries a sign, however it's expected
// that value will not be signed as it represents a absolute delta, the event type can provide the
// context of whether an amount was considered incremental or decremental, i.e. credit or debit to a balance.
func (operation *TransactionOperationWrapper) parseAssetBalanceChangesFromContractEvents() ([]map[string]interface{}, error) {
	balanceChanges := []map[string]interface{}{}

	contractEvents, err := operation.Transaction.GetContractEvents()
	if err != nil {
		// this operation in this context must be an InvokeHostFunctionOp, therefore V3Meta should be present
		// as it's in same soroban model, so if any err, it's real,
		return nil, err
	}

	for _, contractEvent := range contractEvents {
		// Parse the xdr contract event to contractevents.StellarAssetContractEvent model

		// has some convenience like to/from attributes are expressed in strkey format for accounts(G...) and contracts(C...)
		if sacEvent, err := contractevents.NewStellarAssetContractEvent(&contractEvent, operation.Network); err == nil {
			switch sacEvent.GetType() {
			case contractevents.EventTypeTransfer:
				transferEvt := sacEvent.(*contractevents.TransferEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(transferEvt.From, transferEvt.To, transferEvt.Amount, transferEvt.Asset, "transfer"))
			case contractevents.EventTypeMint:
				mintEvt := sacEvent.(*contractevents.MintEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry("", mintEvt.To, mintEvt.Amount, mintEvt.Asset, "mint"))
			case contractevents.EventTypeClawback:
				clawbackEvt := sacEvent.(*contractevents.ClawbackEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(clawbackEvt.From, "", clawbackEvt.Amount, clawbackEvt.Asset, "clawback"))
			case contractevents.EventTypeBurn:
				burnEvt := sacEvent.(*contractevents.BurnEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(burnEvt.From, "", burnEvt.Amount, burnEvt.Asset, "burn"))
			}
		}
	}

	return balanceChanges, nil
}

func parseAssetBalanceChangesFromContractEvents(transaction ingest.LedgerTransaction, network string) ([]map[string]interface{}, error) {
	balanceChanges := []map[string]interface{}{}

	contractEvents, err := transaction.GetContractEvents()
	if err != nil {
		// this operation in this context must be an InvokeHostFunctionOp, therefore V3Meta should be present
		// as it's in same soroban model, so if any err, it's real,
		return nil, err
	}

	for _, contractEvent := range contractEvents {
		// Parse the xdr contract event to contractevents.StellarAssetContractEvent model

		// has some convenience like to/from attributes are expressed in strkey format for accounts(G...) and contracts(C...)
		if sacEvent, err := contractevents.NewStellarAssetContractEvent(&contractEvent, network); err == nil {
			switch sacEvent.GetType() {
			case contractevents.EventTypeTransfer:
				transferEvt := sacEvent.(*contractevents.TransferEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(transferEvt.From, transferEvt.To, transferEvt.Amount, transferEvt.Asset, "transfer"))
			case contractevents.EventTypeMint:
				mintEvt := sacEvent.(*contractevents.MintEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry("", mintEvt.To, mintEvt.Amount, mintEvt.Asset, "mint"))
			case contractevents.EventTypeClawback:
				clawbackEvt := sacEvent.(*contractevents.ClawbackEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(clawbackEvt.From, "", clawbackEvt.Amount, clawbackEvt.Asset, "clawback"))
			case contractevents.EventTypeBurn:
				burnEvt := sacEvent.(*contractevents.BurnEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(burnEvt.From, "", burnEvt.Amount, burnEvt.Asset, "burn"))
			}
		}
	}

	return balanceChanges, nil
}

// fromAccount   - strkey format of contract or address
// toAccount     - strkey format of contract or address, or nillable
// amountChanged - absolute value that asset balance changed
// asset         - the fully qualified issuer:code for asset that had balance change
// changeType    - the type of source sac event that triggered this change
//
// return        - a balance changed record expressed as map of key/value's
func createSACBalanceChangeEntry(fromAccount string, toAccount string, amountChanged xdr.Int128Parts, asset xdr.Asset, changeType string) map[string]interface{} {
	balanceChange := map[string]interface{}{}

	if fromAccount != "" {
		balanceChange["from"] = fromAccount
	}
	if toAccount != "" {
		balanceChange["to"] = toAccount
	}

	balanceChange["type"] = changeType
	balanceChange["amount"] = amount.String128(amountChanged)
	AddAssetDetails(balanceChange, asset, "")
	return balanceChange
}

// addAssetDetails sets the details for `a` on `result` using keys with `prefix`
func AddAssetDetails(result map[string]interface{}, a xdr.Asset, prefix string) error {
	var (
		assetType string
		code      string
		issuer    string
	)
	err := a.Extract(&assetType, &code, &issuer)
	if err != nil {
		err = errors.Wrap(err, "xdr.Asset.Extract error")
		return err
	}
	result[prefix+"asset_type"] = assetType

	if a.Type == xdr.AssetTypeAssetTypeNative {
		return nil
	}

	result[prefix+"asset_code"] = code
	result[prefix+"asset_issuer"] = issuer
	return nil
}

// addAuthFlagDetails adds the account flag details for `f` on `result`.
func addAuthFlagDetails(result map[string]interface{}, f xdr.AccountFlags, prefix string) {
	var (
		n []int32
		s []string
	)

	if f.IsAuthRequired() {
		n = append(n, int32(xdr.AccountFlagsAuthRequiredFlag))
		s = append(s, "auth_required")
	}

	if f.IsAuthRevocable() {
		n = append(n, int32(xdr.AccountFlagsAuthRevocableFlag))
		s = append(s, "auth_revocable")
	}

	if f.IsAuthImmutable() {
		n = append(n, int32(xdr.AccountFlagsAuthImmutableFlag))
		s = append(s, "auth_immutable")
	}

	if f.IsAuthClawbackEnabled() {
		n = append(n, int32(xdr.AccountFlagsAuthClawbackEnabledFlag))
		s = append(s, "auth_clawback_enabled")
	}

	result[prefix+"_flags"] = n
	result[prefix+"_flags_s"] = s
}

// addTrustLineFlagDetails adds the trustline flag details for `f` on `result`.
func addTrustLineFlagDetails(result map[string]interface{}, f xdr.TrustLineFlags, prefix string) {
	var (
		n []int32
		s []string
	)

	if f.IsAuthorized() {
		n = append(n, int32(xdr.TrustLineFlagsAuthorizedFlag))
		s = append(s, "authorized")
	}

	if f.IsAuthorizedToMaintainLiabilitiesFlag() {
		n = append(n, int32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag))
		s = append(s, "authorized_to_maintain_liabilites")
	}

	if f.IsClawbackEnabledFlag() {
		n = append(n, int32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag))
		s = append(s, "clawback_enabled")
	}

	result[prefix+"_flags"] = n
	result[prefix+"_flags_s"] = s
}

func addLedgerKeyDetails(result map[string]interface{}, ledgerKey xdr.LedgerKey) error {
	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeAccount:
		result["account_id"] = ledgerKey.Account.AccountId.Address()
	case xdr.LedgerEntryTypeClaimableBalance:
		marshalHex, err := xdr.MarshalHex(ledgerKey.ClaimableBalance.BalanceId)
		if err != nil {
			return errors.Wrapf(err, "in claimable balance")
		}
		result["claimable_balance_id"] = marshalHex
	case xdr.LedgerEntryTypeData:
		result["data_account_id"] = ledgerKey.Data.AccountId.Address()
		result["data_name"] = ledgerKey.Data.DataName
	case xdr.LedgerEntryTypeOffer:
		result["offer_id"] = fmt.Sprintf("%d", ledgerKey.Offer.OfferId)
	case xdr.LedgerEntryTypeTrustline:
		result["trustline_account_id"] = ledgerKey.TrustLine.AccountId.Address()
		if ledgerKey.TrustLine.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			result["trustline_liquidity_pool_id"] = PoolIDToString(*ledgerKey.TrustLine.Asset.LiquidityPoolId)
		} else {
			result["trustline_asset"] = ledgerKey.TrustLine.Asset.ToAsset().StringCanonical()
		}
	case xdr.LedgerEntryTypeLiquidityPool:
		result["liquidity_pool_id"] = PoolIDToString(ledgerKey.LiquidityPool.LiquidityPoolId)
	}
	return nil
}

func getLedgerKeyParticipants(ledgerKey xdr.LedgerKey) []xdr.AccountId {
	var result []xdr.AccountId
	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeAccount:
		result = append(result, ledgerKey.Account.AccountId)
	case xdr.LedgerEntryTypeClaimableBalance:
		// nothing to do
	case xdr.LedgerEntryTypeData:
		result = append(result, ledgerKey.Data.AccountId)
	case xdr.LedgerEntryTypeOffer:
		result = append(result, ledgerKey.Offer.SellerId)
	case xdr.LedgerEntryTypeTrustline:
		result = append(result, ledgerKey.TrustLine.AccountId)
	}
	return result
}

// Participants returns the accounts taking part in the operation.
func (operation *TransactionOperationWrapper) Participants() ([]xdr.AccountId, error) {
	participants := []xdr.AccountId{}
	participants = append(participants, operation.SourceAccount().ToAccountId())
	op := operation.Operation

	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		participants = append(participants, op.Body.MustCreateAccountOp().Destination)
	case xdr.OperationTypePayment:
		participants = append(participants, op.Body.MustPaymentOp().Destination.ToAccountId())
	case xdr.OperationTypePathPaymentStrictReceive:
		participants = append(participants, op.Body.MustPathPaymentStrictReceiveOp().Destination.ToAccountId())
	case xdr.OperationTypePathPaymentStrictSend:
		participants = append(participants, op.Body.MustPathPaymentStrictSendOp().Destination.ToAccountId())
	case xdr.OperationTypeManageBuyOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeManageSellOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeCreatePassiveSellOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeSetOptions:
		// the only direct participant is the source_account
	case xdr.OperationTypeChangeTrust:
		// the only direct participant is the source_account
	case xdr.OperationTypeAllowTrust:
		participants = append(participants, op.Body.MustAllowTrustOp().Trustor)
	case xdr.OperationTypeAccountMerge:
		participants = append(participants, op.Body.MustDestination().ToAccountId())
	case xdr.OperationTypeInflation:
		// the only direct participant is the source_account
	case xdr.OperationTypeManageData:
		// the only direct participant is the source_account
	case xdr.OperationTypeBumpSequence:
		// the only direct participant is the source_account
	case xdr.OperationTypeCreateClaimableBalance:
		for _, c := range op.Body.MustCreateClaimableBalanceOp().Claimants {
			participants = append(participants, c.MustV0().Destination)
		}
	case xdr.OperationTypeClaimClaimableBalance:
		// the only direct participant is the source_account
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		participants = append(participants, op.Body.MustBeginSponsoringFutureReservesOp().SponsoredId)
	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorshipOp := operation.findInitatingBeginSponsoringOp()
		if beginSponsorshipOp != nil {
			participants = append(participants, beginSponsorshipOp.SourceAccount().ToAccountId())
		}
	case xdr.OperationTypeRevokeSponsorship:
		op := operation.Operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			participants = append(participants, getLedgerKeyParticipants(*op.LedgerKey)...)
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			participants = append(participants, op.Signer.AccountId)
			// We don't add signer as a participant because a signer can be arbitrary account.
			// This can spam successful operations history of any account.
		}
	case xdr.OperationTypeClawback:
		op := operation.Operation.Body.MustClawbackOp()
		participants = append(participants, op.From.ToAccountId())
	case xdr.OperationTypeClawbackClaimableBalance:
		// the only direct participant is the source_account
	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.Operation.Body.MustSetTrustLineFlagsOp()
		participants = append(participants, op.Trustor)
	case xdr.OperationTypeLiquidityPoolDeposit:
		// the only direct participant is the source_account
	case xdr.OperationTypeLiquidityPoolWithdraw:
		// the only direct participant is the source_account
	case xdr.OperationTypeInvokeHostFunction:
		// the only direct participant is the source_account
	case xdr.OperationTypeExtendFootprintTtl:
		// the only direct participant is the source_account
	case xdr.OperationTypeRestoreFootprint:
		// the only direct participant is the source_account
	default:
		return participants, fmt.Errorf("unknown operation type: %s", op.Body.Type)
	}

	sponsor, err := operation.getSponsor()
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		participants = append(participants, *sponsor)
	}

	return dedupeParticipants(participants), nil
}

// dedupeParticipants remove any duplicate ids from `in`
func dedupeParticipants(in []xdr.AccountId) (out []xdr.AccountId) {
	set := map[string]xdr.AccountId{}
	for _, id := range in {
		set[id.Address()] = id
	}

	for _, id := range set {
		out = append(out, id)
	}
	return
}

func serializeParameters(args []xdr.ScVal) ([]map[string]string, []map[string]string) {
	params := make([]map[string]string, 0, len(args))
	paramsDecoded := make([]map[string]string, 0, len(args))

	for _, param := range args {
		serializedParam := map[string]string{}
		serializedParam["value"] = "n/a"
		serializedParam["type"] = "n/a"

		serializedParamDecoded := map[string]string{}
		serializedParamDecoded["value"] = "n/a"
		serializedParamDecoded["type"] = "n/a"

		if scValTypeName, ok := param.ArmForSwitch(int32(param.Type)); ok {
			serializedParam["type"] = scValTypeName
			serializedParamDecoded["type"] = scValTypeName
			if raw, err := param.MarshalBinary(); err == nil {
				serializedParam["value"] = base64.StdEncoding.EncodeToString(raw)
				serializedParamDecoded["value"] = param.String()
			}
		}
		params = append(params, serializedParam)
		paramsDecoded = append(paramsDecoded, serializedParamDecoded)
	}

	return params, paramsDecoded
}

func switchContractIdPreimageType(contractIdPreimage xdr.ContractIdPreimage) map[string]interface{} {
	details := map[string]interface{}{}

	switch contractIdPreimage.Type {
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
		fromAddress := contractIdPreimage.MustFromAddress()
		address, err := fromAddress.Address.String()
		if err != nil {
			panic(fmt.Errorf("error obtaining address for: %s", contractIdPreimage.Type))
		}
		details["from"] = "address"
		details["address"] = address
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
		details["from"] = "asset"
		details["asset"] = contractIdPreimage.MustFromAsset().StringCanonical()
	default:
		panic(fmt.Errorf("unknown contract id type: %s", contractIdPreimage.Type))
	}

	return details
}

type LedgerOperation struct {
	OperationIndex    int32
	Operation         xdr.Operation
	Transaction       *ingest.LedgerTransaction
	NetworkPassphrase string
}

func (o *LedgerOperation) sourceAccountXDR() xdr.MuxedAccount {
	sourceAccount := o.Operation.SourceAccount
	if sourceAccount != nil {
		return *sourceAccount
	}

	return o.Transaction.Envelope.SourceAccount()
}

func (o *LedgerOperation) SourceAccount() string {
	muxedAccount := o.sourceAccountXDR()
	return muxedAccount.ToAccountId().Address()
}

func (o *LedgerOperation) Type() int32 {
	return int32(o.Operation.Body.Type)
}

func (o *LedgerOperation) TypeString() string {
	return xdr.OperationTypeToStringMap[o.Type()]
}

func (o *LedgerOperation) ID() int64 {
	//operationIndex needs +1 increment to stay in sync with ingest package
	return toid.New(int32(o.Transaction.Ledger.LedgerSequence()), int32(o.Transaction.Index), o.OperationIndex+1).ToInt64()
}

func (o *LedgerOperation) SourceAccountMuxed() (string, bool) {
	muxedAccount := o.sourceAccountXDR()
	if muxedAccount.Type != xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		return "", false
	}

	return muxedAccount.Address(), true
}

func (o *LedgerOperation) OperationResultCode() string {
	var operationResultCode string
	operationResults, ok := o.Transaction.Result.Result.OperationResults()
	if ok {
		operationResultCode = operationResults[o.OperationIndex].Code.String()
	}

	return operationResultCode
}

func (o *LedgerOperation) OperationTraceCode() (string, error) {
	var operationTraceCode string
	var operationResults []xdr.OperationResult
	var ok bool
	var err error

	operationResults, ok = o.Transaction.Result.Result.OperationResults()
	if ok {
		var operationResultTr xdr.OperationResultTr
		operationResultTr, ok = operationResults[o.OperationIndex].GetTr()
		if ok {
			operationTraceCode, err = operationResultTr.MapOperationResultTr()
			if err != nil {
				return "", err
			}
			return operationTraceCode, nil
		}
	}

	return operationTraceCode, nil
}

func (o *LedgerOperation) OperationDetails() (interface{}, error) {
	var err error
	var details interface{}

	switch o.Operation.Body.Type {
	case xdr.OperationTypeCreateAccount:
		details, err = o.CreateAccountDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypePayment:
		details, err = o.PaymentDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypePathPaymentStrictReceive:
		details, err = o.PathPaymentStrictReceiveDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypePathPaymentStrictSend:
		details, err = o.PathPaymentStrictSendDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeManageBuyOffer:
		details, err = o.ManageBuyOfferDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeManageSellOffer:
		details, err = o.ManageSellOfferDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeCreatePassiveSellOffer:
		details, err = o.CreatePassiveSellOfferDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeSetOptions:
		details, err = o.SetOptionsDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeChangeTrust:
		details, err = o.ChangeTrustDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeAllowTrust:
		details, err = o.AllowTrustDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeAccountMerge:
		details, err = o.AccountMergeDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeInflation:
		details, err = o.InflationDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeManageData:
		details, err = o.ManageDataDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeBumpSequence:
		details, err = o.BumpSequenceDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeCreateClaimableBalance:
		details, err = o.CreateClaimableBalanceDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeClaimClaimableBalance:
		details, err = o.ClaimClaimableBalanceDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		details, err = o.BeginSponsoringFutureReservesDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeEndSponsoringFutureReserves:
		details, err = o.EndSponsoringFutureReserveDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeRevokeSponsorship:
		details, err = o.RevokeSponsorshipDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeClawback:
		details, err = o.ClawbackDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeClawbackClaimableBalance:
		details, err = o.ClawbackClaimableBalanceDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeSetTrustLineFlags:
		details, err = o.SetTrustlineFlagsDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeLiquidityPoolDeposit:
		details, err = o.LiquidityPoolDepositDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeLiquidityPoolWithdraw:
		details, err = o.LiquidityPoolWithdrawDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeInvokeHostFunction:
		details, err = o.InvokeHostFunctionDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeExtendFootprintTtl:
		details, err = o.ExtendFootprintTtlDetails()
		if err != nil {
			return details, err
		}
	case xdr.OperationTypeRestoreFootprint:
		details, err = o.RestoreFootprintDetails()
		if err != nil {
			return details, err
		}
	default:
		return details, fmt.Errorf("unknown operation type: %s", o.Operation.Body.Type.String())
	}

	return details, nil
}

func getMuxedAccountDetails(a xdr.MuxedAccount) (string, uint64, error) {
	var err error
	var muxedAccountAddress string
	var muxedAccountID uint64

	if a.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		muxedAccountAddress, err = a.GetAddress()
		if err != nil {
			return "", 0, err
		}
		muxedAccountID, err = a.GetId()
		if err != nil {
			return "", 0, err
		}
	}
	return muxedAccountAddress, muxedAccountID, nil
}

type LedgerKeyDetail struct {
	AccountID                string `json:"account_id"`
	ClaimableBalanceID       string `json:"claimable_balance_id"`
	DataAccountID            string `json:"data_account_id"`
	DataName                 string `json:"data_name"`
	OfferID                  int64  `json:"offer_id,string"`
	TrustlineAccountID       string `json:"trustline_account_id"`
	TrustlineLiquidityPoolID string `json:"trustline_liquidity_pool_id"`
	TrustlineAssetCode       string `json:"trustline_asset_code"`
	TrustlineAssetIssuer     string `json:"trustline_asset_issuer"`
	TrustlineAssetType       string `json:"trustline_asset_type"`
	LiquidityPoolID          string `json:"liquidity_pool_id"`
}

func addLedgerKey(ledgerKey xdr.LedgerKey) (LedgerKeyDetail, error) {
	var err error
	var ledgerKeyDetail LedgerKeyDetail

	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeAccount:
		ledgerKeyDetail.AccountID = ledgerKey.Account.AccountId.Address()
	case xdr.LedgerEntryTypeClaimableBalance:
		var marshalHex string
		marshalHex, err = xdr.MarshalHex(ledgerKey.ClaimableBalance.BalanceId)
		if err != nil {
			return LedgerKeyDetail{}, fmt.Errorf("in claimable balance: %w", err)
		}
		ledgerKeyDetail.ClaimableBalanceID = marshalHex
	case xdr.LedgerEntryTypeData:
		ledgerKeyDetail.DataAccountID = ledgerKey.Data.AccountId.Address()
		ledgerKeyDetail.DataName = string(ledgerKey.Data.DataName)
	case xdr.LedgerEntryTypeOffer:
		ledgerKeyDetail.OfferID = int64(ledgerKey.Offer.OfferId)
	case xdr.LedgerEntryTypeTrustline:
		ledgerKeyDetail.TrustlineAccountID = ledgerKey.TrustLine.AccountId.Address()
		if ledgerKey.TrustLine.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			ledgerKeyDetail.TrustlineLiquidityPoolID = PoolIDToString(*ledgerKey.TrustLine.Asset.LiquidityPoolId)
		} else {
			var assetCode, assetIssuer, assetType string
			err = ledgerKey.TrustLine.Asset.ToAsset().Extract(&assetType, &assetCode, &assetIssuer)
			if err != nil {
				return LedgerKeyDetail{}, err
			}

			ledgerKeyDetail.TrustlineAssetCode = assetCode
			ledgerKeyDetail.TrustlineAssetIssuer = assetIssuer
			ledgerKeyDetail.TrustlineAssetType = assetType
		}
	case xdr.LedgerEntryTypeLiquidityPool:
		ledgerKeyDetail.LiquidityPoolID = PoolIDToString(ledgerKey.LiquidityPool.LiquidityPoolId)
	}

	return ledgerKeyDetail, nil
}

func (o *LedgerOperation) getLiquidityPoolAndProductDelta(lpID *xdr.PoolId) (*xdr.LiquidityPoolEntry, *LiquidityPoolDelta, error) {
	changes, err := o.Transaction.GetOperationChanges(uint32(o.OperationIndex))
	if err != nil {
		return nil, nil, err
	}

	for _, c := range changes {
		if c.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}
		// The delta can be caused by a full removal or full creation of the liquidity pool
		var lp *xdr.LiquidityPoolEntry
		var preA, preB, preShares xdr.Int64
		if c.Pre != nil {
			if lpID != nil && c.Pre.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Pre.Data.LiquidityPool
			if c.Pre.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Pre.Data.LiquidityPool.Body.Type)
			}
			cpPre := c.Pre.Data.LiquidityPool.Body.ConstantProduct
			preA, preB, preShares = cpPre.ReserveA, cpPre.ReserveB, cpPre.TotalPoolShares
		}
		var postA, postB, postShares xdr.Int64
		if c.Post != nil {
			if lpID != nil && c.Post.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Post.Data.LiquidityPool
			if c.Post.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Post.Data.LiquidityPool.Body.Type)
			}
			cpPost := c.Post.Data.LiquidityPool.Body.ConstantProduct
			postA, postB, postShares = cpPost.ReserveA, cpPost.ReserveB, cpPost.TotalPoolShares
		}
		delta := &LiquidityPoolDelta{
			ReserveA:        postA - preA,
			ReserveB:        postB - preB,
			TotalPoolShares: postShares - preShares,
		}
		return lp, delta, nil
	}

	return nil, nil, fmt.Errorf("liquidity pool change not found")
}

func (o *LedgerOperation) serializeParameters(args []xdr.ScVal) ([]interface{}, error) {
	var params []interface{}

	for _, param := range args {
		if _, ok := param.ArmForSwitch(int32(param.Type)); ok {
			var err error
			var raw []byte
			raw, err = param.MarshalBinary()
			if err != nil {
				return nil, err
			}

			params = append(params, raw)
		}
	}

	return params, nil
}

func (o *LedgerOperation) parseAssetBalanceChangesFromContractEvents() ([]BalanceChangeDetail, error) {
	balanceChanges := []BalanceChangeDetail{}

	contractEvents, err := o.Transaction.GetContractEvents()
	if err != nil {
		// this operation in this context must be an InvokeHostFunctionOp, therefore V3Meta should be present
		// as it's in same soroban model, so if any err, it's real,
		return nil, err
	}

	for _, contractEvent := range contractEvents {
		// Parse the xdr contract event to contractevents.StellarAssetContractEvent model

		var err error
		var balanceChangeDetail BalanceChangeDetail
		var sacEvent contractevents.StellarAssetContractEvent
		// has some convenience like to/from attributes are expressed in strkey format for accounts(G...) and contracts(C...)
		if sacEvent, err = contractevents.NewStellarAssetContractEvent(&contractEvent, o.NetworkPassphrase); err == nil {
			switch sacEvent.GetType() {
			case contractevents.EventTypeTransfer:
				transferEvt := sacEvent.(*contractevents.TransferEvent)
				balanceChangeDetail, err = createSACBalanceChange(transferEvt.From, transferEvt.To, transferEvt.Amount, transferEvt.Asset, "transfer")
				if err != nil {
					return []BalanceChangeDetail{}, err
				}

				balanceChanges = append(balanceChanges, balanceChangeDetail)
			case contractevents.EventTypeMint:
				mintEvt := sacEvent.(*contractevents.MintEvent)
				balanceChangeDetail, err = createSACBalanceChange("", mintEvt.To, mintEvt.Amount, mintEvt.Asset, "mint")
				if err != nil {
					return []BalanceChangeDetail{}, err
				}

				balanceChanges = append(balanceChanges, balanceChangeDetail)
			case contractevents.EventTypeClawback:
				clawbackEvt := sacEvent.(*contractevents.ClawbackEvent)
				balanceChangeDetail, err = createSACBalanceChange(clawbackEvt.From, "", clawbackEvt.Amount, clawbackEvt.Asset, "clawback")
				if err != nil {
					return []BalanceChangeDetail{}, err
				}

				balanceChanges = append(balanceChanges, balanceChangeDetail)
			case contractevents.EventTypeBurn:
				burnEvt := sacEvent.(*contractevents.BurnEvent)
				balanceChangeDetail, err = createSACBalanceChange(burnEvt.From, "", burnEvt.Amount, burnEvt.Asset, "burn")
				if err != nil {
					return []BalanceChangeDetail{}, err
				}

				balanceChanges = append(balanceChanges, balanceChangeDetail)
			}
		}
	}

	return balanceChanges, nil
}

type BalanceChangeDetail struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Type        string `json:"type"`
	Amount      string `json:"amount"`
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
}

// fromAccount   - strkey format of contract or address
// toAccount     - strkey format of contract or address, or nillable
// amountChanged - absolute value that asset balance changed
// asset         - the fully qualified issuer:code for asset that had balance change
// changeType    - the type of source sac event that triggered this change
//
// return        - a balance changed record expressed as map of key/value's
func createSACBalanceChange(fromAccount string, toAccount string, amountChanged xdr.Int128Parts, asset xdr.Asset, changeType string) (BalanceChangeDetail, error) {
	balanceChangeDetail := BalanceChangeDetail{
		Type:   changeType,
		Amount: amount.String128(amountChanged),
	}

	if fromAccount != "" {
		balanceChangeDetail.From = fromAccount
	}
	if toAccount != "" {
		balanceChangeDetail.To = toAccount
	}

	var assetCode, assetIssuer, assetType string
	err := asset.Extract(&assetType, &assetCode, &assetIssuer)
	if err != nil {
		return BalanceChangeDetail{}, err
	}

	return balanceChangeDetail, nil
}

type PreImageDetails struct {
	From        string `json:"from"`
	Address     string `json:"address"`
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
}

func switchContractIdPreimage(contractIdPreimage xdr.ContractIdPreimage) (PreImageDetails, error) {
	switch contractIdPreimage.Type {
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
		fromAddress := contractIdPreimage.MustFromAddress()
		address, err := fromAddress.Address.String()
		if err != nil {
			return PreImageDetails{}, err
		}
		return PreImageDetails{
			From:    "address",
			Address: address,
		}, nil
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
		var assetCode, assetIssuer, assetType string
		contractIdPreimage.MustFromAsset().Extract(&assetType, &assetCode, &assetIssuer)

		return PreImageDetails{
			From:        "asset",
			AssetCode:   assetCode,
			AssetIssuer: assetIssuer,
			AssetType:   assetType,
		}, nil

	default:
		return PreImageDetails{}, fmt.Errorf("unknown contract id type: %s", contractIdPreimage.Type)
	}
}

func (o *LedgerOperation) ConvertStroopValueToReal(input int64) float64 {
	output, _ := big.NewRat(int64(input), int64(10000000)).Float64()
	return output
}

func (o *LedgerOperation) FormatPrefix(p string) string {
	if p != "" {
		p += "_"
	}
	return p
}

func (o *LedgerOperation) FarmHashAsset(assetCode, assetIssuer, assetType string) int64 {
	asset := fmt.Sprintf("%s%s%s", assetCode, assetIssuer, assetType)
	hash := farm.Fingerprint64([]byte(asset))

	return int64(hash)
}

// Path is a representation of an asset without an ID that forms part of a path in a path payment
type Path struct {
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
}

func (o *LedgerOperation) TransformPath(initialPath []xdr.Asset) []Path {
	if len(initialPath) == 0 {
		return nil
	}
	var path = make([]Path, 0)
	for _, pathAsset := range initialPath {
		var assetType, code, issuer string
		err := pathAsset.Extract(&assetType, &code, &issuer)
		if err != nil {
			return nil
		}

		path = append(path, Path{
			AssetType:   assetType,
			AssetIssuer: issuer,
			AssetCode:   code,
		})
	}
	return path
}

type Price struct {
	Numerator   int32 `json:"n"`
	Denominator int32 `json:"d"`
}

type Claimant struct {
	Destination string             `json:"destination"`
	Predicate   xdr.ClaimPredicate `json:"predicate"`
}

func transformClaimants(claimants []xdr.Claimant) []Claimant {
	var transformed []Claimant
	for _, c := range claimants {
		switch c.Type {
		case 0:
			transformed = append(transformed, Claimant{
				Destination: c.V0.Destination.Address(),
				Predicate:   c.V0.Predicate,
			})
		}
	}
	return transformed
}

type SponsorshipOutput struct {
	Operation      xdr.Operation
	OperationIndex uint32
}

type LiquidityPoolDelta struct {
	ReserveA        xdr.Int64
	ReserveB        xdr.Int64
	TotalPoolShares xdr.Int64
}
