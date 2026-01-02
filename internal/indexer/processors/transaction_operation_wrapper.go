// TransactionOperationWrapper and related functionality.
package processors

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Claimant represents a claimant for a claimable balance
type Claimant struct {
	Destination string             `json:"destination"`
	Predicate   xdr.ClaimPredicate `json:"predicate"`
}

// ErrLiquidityPoolChangeNotFound is returned when a liquidity pool change is not found
var ErrLiquidityPoolChangeNotFound = errors.New("liquidity pool change not found")

// TransactionOperationWrapper represents the data for a single operation within a transaction
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
		return nil, fmt.Errorf("getting operation changes: %w", err)
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
			if err := AddAssetDetails(details, op.Line.ToAsset(), ""); err != nil {
				return nil, err
			}
			details["trustee"] = details["asset_issuer"]
		}
		if err := AddAccountAndMuxedAccountDetails(details, *source, "trustor"); err != nil {
			return nil, err
		}
		details["limit"] = amount.String(op.Limit)
	case xdr.OperationTypeAllowTrust:
		op := operation.Operation.Body.MustAllowTrustOp()
		if err := AddAssetDetails(details, op.Asset.ToAsset(source.ToAccountId()), ""); err != nil {
			return nil, err
		}
		if err := AddAccountAndMuxedAccountDetails(details, *source, "trustee"); err != nil {
			return nil, err
		}
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
	case xdr.OperationTypeManageData:
		op := operation.Operation.Body.MustManageDataOp()
		details["name"] = string(op.DataName)
		if op.DataValue != nil {
			details["value"] = base64.StdEncoding.EncodeToString(*op.DataValue)
		} else {
			details["value"] = nil
		}
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op := operation.Operation.Body.MustBeginSponsoringFutureReservesOp()
		details["sponsored_id"] = op.SponsoredId.Address()
	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorshipOp := operation.findInitatingBeginSponsoringOp()
		if beginSponsorshipOp != nil {
			beginSponsorshipSource := beginSponsorshipOp.SourceAccount()
			if err := AddAccountAndMuxedAccountDetails(details, *beginSponsorshipSource, "begin_sponsor"); err != nil {
				return nil, err
			}
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
	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.Operation.Body.MustSetTrustLineFlagsOp()
		details["trustor"] = op.Trustor.Address()
		if err := AddAssetDetails(details, op.Asset, ""); err != nil {
			return nil, err
		}
		if op.SetFlags > 0 {
			addTrustLineFlagDetails(details, xdr.TrustLineFlags(op.SetFlags), "set")
		}

		if op.ClearFlags > 0 {
			addTrustLineFlagDetails(details, xdr.TrustLineFlags(op.ClearFlags), "clear")
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

			contractID, err := invokeArgs.ContractAddress.String()
			if err != nil {
				return nil, fmt.Errorf("converting contract address to string: %w", err)
			}

			transactionEnvelope := getTransactionV1Envelope(operation.Transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractID
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			details["parameters"], details["parameters_decoded"] = serializeParameters(args)

		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			args := op.HostFunction.MustCreateContract()
			details["type"] = "create_contract"

			transactionEnvelope := getTransactionV1Envelope(operation.Transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractIDFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			preimageTypeMap := switchContractIDPreimageType(args.ContractIdPreimage)
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
			details["contract_id"] = contractIDFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			// ConstructorArgs is a list of ScVals
			// This will initially be handled the same as InvokeContractParams until a different
			// model is found necessary.
			constructorArgs := args.ConstructorArgs
			details["parameters"], details["parameters_decoded"] = serializeParameters(constructorArgs)

			preimageTypeMap := switchContractIDPreimageType(args.ContractIdPreimage)
			for key, val := range preimageTypeMap {
				if _, ok := preimageTypeMap[key]; ok {
					details[key] = val
				}
			}
		default:
			panic(fmt.Errorf("unknown host function type: %s", op.HostFunction.Type))
		}
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

func contractIDFromTxEnvelope(transactionEnvelope xdr.TransactionV1Envelope) string {
	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		contractID := contractIDFromContractData(ledgerKey)
		if contractID != "" {
			return contractID
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly {
		contractID := contractIDFromContractData(ledgerKey)
		if contractID != "" {
			return contractID
		}
	}

	return ""
}

func contractIDFromContractData(ledgerKey xdr.LedgerKey) string {
	contractData, ok := ledgerKey.GetContractData()
	if !ok {
		return ""
	}
	contractIDHash, ok := contractData.Contract.GetContractId()
	if !ok {
		return ""
	}

	contractIDByte, err := contractIDHash.MarshalBinary()
	if err != nil {
		return ""
	}
	contractID, err := strkey.Encode(strkey.VersionByteContract, contractIDByte)
	if err != nil {
		return ""
	}
	return contractID
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
		if LedgerKeyToLedgerKeyHash(ledgerKey) != "" {
			ledgerKeyHash = append(ledgerKeyHash, LedgerKeyToLedgerKeyHash(ledgerKey))
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		if LedgerKeyToLedgerKeyHash(ledgerKey) != "" {
			ledgerKeyHash = append(ledgerKeyHash, LedgerKeyToLedgerKeyHash(ledgerKey))
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

func switchContractIDPreimageType(contractIDPreimage xdr.ContractIdPreimage) map[string]interface{} {
	details := map[string]interface{}{}

	switch contractIDPreimage.Type {
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
		fromAddress := contractIDPreimage.MustFromAddress()
		address, err := fromAddress.Address.String()
		if err != nil {
			panic(fmt.Errorf("error obtaining address for: %s", contractIDPreimage.Type))
		}
		details["from"] = "address"
		details["address"] = address
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
		details["from"] = "asset"
		details["asset"] = contractIDPreimage.MustFromAsset().StringCanonical()
	default:
		panic(fmt.Errorf("unknown contract id type: %s", contractIDPreimage.Type))
	}

	return details
}
