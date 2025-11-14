package processors

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/hash"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/protocols/horizon/base"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/contractevents"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
)

// Claimant represents a claimant for a claimable balance
type Claimant struct {
	Destination string             `json:"destination"`
	Predicate   xdr.ClaimPredicate `json:"predicate"`
}

// LedgerKeyToLedgerKeyHash converts a ledger key to its hash representation
func LedgerKeyToLedgerKeyHash(ledgerKey xdr.LedgerKey) string {
	ledgerKeyByte, err := ledgerKey.MarshalBinary()
	if err != nil {
		return ""
	}
	hashedLedgerKeyByte := hash.Hash(ledgerKeyByte)
	ledgerKeyHash := hex.EncodeToString(hashedLedgerKeyByte[:])

	return ledgerKeyHash
}

type liquidityPoolDelta struct {
	ReserveA        xdr.Int64
	ReserveB        xdr.Int64
	TotalPoolShares xdr.Int64
}

func PoolIDToString(id xdr.PoolId) string {
	return xdr.Hash(id).HexString()
}

func formatPrefix(p string) string {
	if p != "" {
		p += "_"
	}
	return p
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

		path := make([]map[string]interface{}, len(op.Path))
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

		path := make([]map[string]interface{}, len(op.Path))
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
		var claimants []Claimant
		for _, c := range op.Claimants {
			cv0 := c.MustV0()
			claimants = append(claimants, Claimant{
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
