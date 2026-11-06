package services

import (
	"fmt"
	"math"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
)

// maxAccountSigners is the ledger's cap on extra signers per account (the
// master key is not in the list): AccountEntry declares `Signer signers<20>`,
// and Core rejects the 21st with SET_OPTIONS_TOO_MANY_SIGNERS.
const maxAccountSigners = 20

// ledgerTransactionFromClassic builds the simulated ledger transaction for a
// classic transaction. RPC cannot simulate classic operations, so this path
// derives the outcome itself in three steps: list the ledger entries the
// operation touches (classicFootprint), fetch their current state from RPC
// (fetchLedgerEntries), and apply the operation's stated effect to those
// entries (computeClassicChanges). The resulting before/after entries feed
// the same synthesis and processors as the Soroban path.
//
// Scope: one operation per transaction, and only the operation types
// classicFootprint lists. Transactions with several operations need the
// evolving-state driver (phase 3) and are rejected as unsupported for now.
func (s *transactionSimulationService) ledgerTransactionFromClassic(envelope xdr.TransactionEnvelope) (ingest.LedgerTransaction, uint32, error) {
	if envelope.Type == xdr.EnvelopeTypeEnvelopeTypeTxFeeBump {
		return ingest.LedgerTransaction{}, 0, fmt.Errorf("%w: fee-bump classic transactions are not supported yet", ErrUnsupportedTransaction)
	}
	ops := envelope.Operations()
	if len(ops) > 1 {
		return ingest.LedgerTransaction{}, 0, fmt.Errorf("%w: multi-operation classic transactions are not supported yet", ErrUnsupportedTransaction)
	}
	op := ops[0]
	if err := validateClassicOperation(op); err != nil {
		return ingest.LedgerTransaction{}, 0, err
	}
	opSource := classicOperationSource(envelope, op)
	txSource := envelope.SourceAccount().ToAccountId()

	keys, err := classicFootprint(op, opSource)
	if err != nil {
		return ingest.LedgerTransaction{}, 0, err
	}
	// The transaction source always participates: it pays the fee regardless
	// of which account the operation acts on.
	keys = append(keys, accountLedgerKey(txSource))
	before, latestLedger, err := s.fetchLedgerEntries(keys)
	if err != nil {
		return ingest.LedgerTransaction{}, 0, err
	}

	// The network rejects a transaction whose bid is below the base fee per
	// operation, or whose source cannot cover the full bid, before the
	// operation ever runs.
	fee := int64(envelope.Fee())
	feeCharged := int64(len(envelope.Operations())) * baseFeeStroops
	if fee < feeCharged {
		return ingest.LedgerTransaction{}, 0, wouldFail("fee bid %d is below the network minimum %d", fee, feeCharged)
	}
	feeSource, ok := lookupAccount(before, txSource)
	if !ok {
		return ingest.LedgerTransaction{}, 0, wouldFail("transaction source account %s does not exist", txSource.Address())
	}
	if accountSpendableBalance(feeSource) < fee {
		return ingest.LedgerTransaction{}, 0, wouldFail("transaction source account %s cannot cover the %d stroop fee", txSource.Address(), fee)
	}

	// Outside surge pricing, Core charges exactly the base fee per operation,
	// not the bid. Charge it up front by debiting the fee source in the before
	// image, the order Core applies it in, so every handler computes against
	// the post-fee balance without threading the fee through.
	feeSource.Balance -= xdr.Int64(feeCharged)
	storeAccount(before, feeSource)

	changes, err := computeClassicChanges(op, opSource, before, latestLedger)
	if err != nil {
		return ingest.LedgerTransaction{}, 0, err
	}

	opResults, err := successOperationResults(envelope)
	if err != nil {
		return ingest.LedgerTransaction{}, 0, err
	}
	tx := newSimulatedLedgerTransaction(envelope, latestLedger, feeCharged, changes, nil, opResults)
	return tx, latestLedger, nil
}

// validateClassicOperation rejects operations whose fields violate protocol
// rules before any state is considered, so malformed (but decodable) XDR can
// never produce a preview: a negative payment amount would otherwise reverse
// the balance movement, and out-of-range thresholds would be silently
// truncated to bytes.
func validateClassicOperation(op xdr.Operation) error {
	switch op.Body.Type {
	case xdr.OperationTypePayment:
		if op.Body.PaymentOp.Amount <= 0 {
			return wouldFail("payment amount must be positive, got %d", op.Body.PaymentOp.Amount)
		}
		if !creditAssetCodeValid(op.Body.PaymentOp.Asset) {
			return wouldFail("payment asset code is malformed")
		}
	case xdr.OperationTypeChangeTrust:
		if op.Body.ChangeTrustOp.Limit < 0 {
			return wouldFail("trustline limit cannot be negative, got %d", op.Body.ChangeTrustOp.Limit)
		}
		if asset, ok := changeTrustCreditAsset(op.Body.ChangeTrustOp.Line); ok && !creditAssetCodeValid(asset) {
			return wouldFail("trustline asset code is malformed")
		}
	case xdr.OperationTypeSetOptions:
		so := op.Body.SetOptionsOp
		for _, threshold := range []struct {
			name  string
			value *xdr.Uint32
		}{
			{"masterWeight", so.MasterWeight},
			{"lowThreshold", so.LowThreshold},
			{"medThreshold", so.MedThreshold},
			{"highThreshold", so.HighThreshold},
		} {
			if threshold.value != nil && *threshold.value > 255 {
				return wouldFail("%s %d is out of range (0-255)", threshold.name, *threshold.value)
			}
		}
		if so.Signer != nil && so.Signer.Weight > 255 {
			return wouldFail("signer weight %d is out of range (0-255)", so.Signer.Weight)
		}
		if so.SetFlags != nil && *so.SetFlags&^allAccountAuthFlags != 0 {
			return wouldFail("setFlags %#x contains unknown account flags", *so.SetFlags)
		}
		if so.ClearFlags != nil && *so.ClearFlags&^allAccountAuthFlags != 0 {
			return wouldFail("clearFlags %#x contains unknown account flags", *so.ClearFlags)
		}
		if so.SetFlags != nil && so.ClearFlags != nil && *so.SetFlags&*so.ClearFlags != 0 {
			return wouldFail("setFlags and clearFlags overlap on %#x", *so.SetFlags&*so.ClearFlags)
		}
	case xdr.OperationTypeManageData:
		// Core requires a non-empty name of printable ASCII (0x20-0x7E).
		name := string(op.Body.ManageDataOp.DataName)
		if len(name) == 0 {
			return wouldFail("data entry name cannot be empty")
		}
		for _, c := range []byte(name) {
			if c <= 0x1f || c >= 0x7f {
				return wouldFail("data entry name contains non-printable characters")
			}
		}
	default:
		// createAccount's starting balance is validated against the minimum
		// reserve in its handler.
	}
	return nil
}

// allAccountAuthFlags is every account flag the protocol defines; setOptions
// rejects set or clear masks outside it (SET_OPTIONS_UNKNOWN_FLAG).
const allAccountAuthFlags = xdr.Uint32(xdr.AccountFlagsAuthRequiredFlag |
	xdr.AccountFlagsAuthRevocableFlag |
	xdr.AccountFlagsAuthImmutableFlag |
	xdr.AccountFlagsAuthClawbackEnabledFlag)

// creditAssetCodeValid mirrors Core's isAssetValid for credit assets: ASCII
// alphanumeric characters with only trailing zero padding, 1-4 characters for
// alphanum4 and 5-12 for alphanum12.
func creditAssetCodeValid(asset xdr.Asset) bool {
	var code []byte
	var minLen int
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return true
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		code, minLen = asset.AlphaNum4.AssetCode[:], 1
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		code, minLen = asset.AlphaNum12.AssetCode[:], 5
	default:
		return false
	}
	length, padded := 0, false
	for _, c := range code {
		switch {
		case c == 0:
			padded = true
		case padded:
			return false
		case ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9'):
			length++
		default:
			return false
		}
	}
	return length >= minLen
}

// classicOperationSource returns the account the operation acts on behalf of:
// the operation's own source account when it sets one, otherwise the
// transaction's source account.
func classicOperationSource(envelope xdr.TransactionEnvelope, op xdr.Operation) xdr.AccountId {
	if op.SourceAccount != nil {
		return op.SourceAccount.ToAccountId()
	}
	return envelope.SourceAccount().ToAccountId()
}

// classicFootprint lists the ledger entries the operation would read or write,
// derived from the operation's fields alone, before any state is fetched. Every
// supported operation touches a small, known set of entries: a native payment
// touches the sender's and receiver's account entries, a USDC payment also
// touches their two USDC trustline entries, and so on. Operation types whose
// outcome depends on order-book or pool matching cannot be predicted this way
// and are rejected as unsupported.
func classicFootprint(op xdr.Operation, opSource xdr.AccountId) ([]xdr.LedgerKey, error) {
	switch op.Body.Type {
	case xdr.OperationTypePayment:
		p := op.Body.PaymentOp
		keys := []xdr.LedgerKey{accountLedgerKey(opSource), accountLedgerKey(p.Destination.ToAccountId())}
		if p.Asset.Type != xdr.AssetTypeAssetTypeNative {
			issuer := p.Asset.GetIssuer()
			if opSource.Address() != issuer {
				keys = append(keys, trustlineLedgerKey(opSource, p.Asset))
			}
			if dst := p.Destination.ToAccountId(); dst.Address() != issuer {
				keys = append(keys, trustlineLedgerKey(dst, p.Asset))
			}
		}
		return keys, nil

	case xdr.OperationTypeCreateAccount:
		return []xdr.LedgerKey{
			accountLedgerKey(opSource),
			accountLedgerKey(op.Body.CreateAccountOp.Destination),
		}, nil

	case xdr.OperationTypeChangeTrust:
		ct := op.Body.ChangeTrustOp
		asset, ok := changeTrustCreditAsset(ct.Line)
		if !ok {
			return nil, fmt.Errorf("%w: liquidity pool share trustlines are not supported", ErrUnsupportedTransaction)
		}
		// The issuer account is fetched to derive the new trustline's starting
		// authorization from the issuer's AUTH_REQUIRED flag.
		issuer := xdr.MustAddress(asset.GetIssuer())
		return []xdr.LedgerKey{
			accountLedgerKey(opSource),
			trustlineLedgerKey(opSource, asset),
			accountLedgerKey(issuer),
		}, nil

	case xdr.OperationTypeSetOptions:
		keys := []xdr.LedgerKey{accountLedgerKey(opSource)}
		// Setting an inflation destination requires that account to exist, so
		// fetch it for the handler's existence check.
		if dest := op.Body.SetOptionsOp.InflationDest; dest != nil {
			keys = append(keys, accountLedgerKey(*dest))
		}
		return keys, nil

	case xdr.OperationTypeManageData:
		return []xdr.LedgerKey{
			accountLedgerKey(opSource),
			dataLedgerKey(opSource, string(op.Body.ManageDataOp.DataName)),
		}, nil

	default:
		return nil, fmt.Errorf("%w: classic operation type %s is not supported", ErrUnsupportedTransaction, op.Body.Type)
	}
}

// fetchLedgerEntries fetches the footprint's entries from RPC and returns them
// keyed by their base64-encoded ledger key, along with the latest ledger the
// node evaluated. An entry that does not exist on the ledger is simply absent
// from the map; each handler decides whether absence means "the operation will
// create it" or "the transaction would fail".
func (s *transactionSimulationService) fetchLedgerEntries(keys []xdr.LedgerKey) (map[string]xdr.LedgerEntry, uint32, error) {
	seen := make(map[string]struct{}, len(keys))
	encoded := make([]string, 0, len(keys))
	for _, key := range keys {
		b64, err := xdr.MarshalBase64(key)
		if err != nil {
			return nil, 0, fmt.Errorf("encoding ledger key: %w", err)
		}
		if _, ok := seen[b64]; ok {
			continue
		}
		seen[b64] = struct{}{}
		encoded = append(encoded, b64)
	}

	result, err := s.rpcService.GetLedgerEntries(encoded)
	if err != nil {
		return nil, 0, fmt.Errorf("fetching ledger entries via RPC: %w", err)
	}

	entries := make(map[string]xdr.LedgerEntry, len(result.Entries))
	for _, entry := range result.Entries {
		var entryData xdr.LedgerEntryData
		if err := xdr.SafeUnmarshalBase64(entry.DataXDR, &entryData); err != nil {
			return nil, 0, fmt.Errorf("decoding ledger entry data: %w", err)
		}
		entries[entry.KeyXDR] = xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(entry.LastModifiedLedger),
			Data:                  entryData,
		}
	}
	return entries, result.LatestLedger, nil
}

// computeClassicChanges applies the operation's stated effect to the fetched
// before-entries and returns before/after change pairs in the same shape a real
// ledger's transaction meta carries, so the existing processors can read them
// unchanged. When the current state means the network would reject the
// operation (for example paying more than the balance), it returns
// ErrSimulationFailed instead, so a preview is never shown for a transaction
// that would not succeed. The fee has already been debited from the before
// image, so handlers see the balance the operation would actually run against.
func computeClassicChanges(op xdr.Operation, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, ledgerSeq uint32) (xdr.LedgerEntryChanges, error) {
	switch op.Body.Type {
	case xdr.OperationTypePayment:
		return computePaymentChanges(*op.Body.PaymentOp, opSource, before)
	case xdr.OperationTypeCreateAccount:
		return computeCreateAccountChanges(*op.Body.CreateAccountOp, opSource, before, ledgerSeq)
	case xdr.OperationTypeChangeTrust:
		return computeChangeTrustChanges(*op.Body.ChangeTrustOp, opSource, before)
	case xdr.OperationTypeSetOptions:
		return computeSetOptionsChanges(*op.Body.SetOptionsOp, opSource, before)
	case xdr.OperationTypeManageData:
		return computeManageDataChanges(*op.Body.ManageDataOp, opSource, before)
	default:
		// classicFootprint already rejected unsupported types; this is a guard
		// against the two switches drifting apart.
		return nil, fmt.Errorf("%w: classic operation type %s is not supported", ErrUnsupportedTransaction, op.Body.Type)
	}
}

func computePaymentChanges(p xdr.PaymentOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry) (xdr.LedgerEntryChanges, error) {
	dst := p.Destination.ToAccountId()
	srcAccount, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	dstAccount, ok := lookupAccount(before, dst)
	if !ok {
		return nil, wouldFail("destination account %s does not exist", dst.Address())
	}

	if p.Asset.Type == xdr.AssetTypeAssetTypeNative {
		if opSource.Equals(dst) {
			// A self-payment moves nothing; emit no entry changes. The
			// token-transfer processor still derives the debit/credit pair
			// from the operation itself, matching history.
			return xdr.LedgerEntryChanges{}, nil
		}
		available := accountSpendableBalance(srcAccount)
		if available < int64(p.Amount) {
			return nil, wouldFail("source account %s has insufficient XLM: available %d stroops, sending %d", opSource.Address(), available, p.Amount)
		}
		// Core's line-full rule for the native destination: the balance plus
		// open buy-offer liabilities must stay within the int64 ceiling.
		if int64(p.Amount) > math.MaxInt64-int64(dstAccount.Balance)-int64(dstAccount.Liabilities().Buying) {
			return nil, wouldFail("destination account %s cannot receive %d stroops without exceeding the maximum balance", dst.Address(), p.Amount)
		}
		srcAfter := srcAccount
		srcAfter.Balance -= p.Amount
		dstAfter := dstAccount
		dstAfter.Balance += p.Amount
		return append(
			accountChangePair(srcAccount, srcAfter),
			accountChangePair(dstAccount, dstAfter)...,
		), nil
	}

	// A non-native payment moves value between trustline entries, not account
	// entries. The issuer holds no trustline for its own asset, so a payment
	// from the issuer creates new units and a payment to the issuer destroys
	// them; the issuer's side of the movement is simply skipped.
	issuer := p.Asset.GetIssuer()
	var changes xdr.LedgerEntryChanges
	if opSource.Address() != issuer {
		srcLine, ok := lookupTrustline(before, opSource, p.Asset)
		if !ok {
			return nil, wouldFail("source account %s holds no trustline for %s", opSource.Address(), p.Asset.StringCanonical())
		}
		if srcLine.Flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag) == 0 {
			return nil, wouldFail("source trustline for %s is not authorized", p.Asset.StringCanonical())
		}
		available := int64(srcLine.Balance) - int64(srcLine.Liabilities().Selling)
		if available < int64(p.Amount) {
			return nil, wouldFail("source trustline for %s has insufficient balance: available %d, sending %d", p.Asset.StringCanonical(), available, p.Amount)
		}
		srcAfter := srcLine
		srcAfter.Balance -= p.Amount
		changes = append(changes, trustlineChangePair(srcLine, srcAfter)...)
	}
	if dst.Address() != issuer {
		dstLine, ok := lookupTrustline(before, dst, p.Asset)
		if !ok {
			return nil, wouldFail("destination account %s holds no trustline for %s", dst.Address(), p.Asset.StringCanonical())
		}
		if dstLine.Flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag) == 0 {
			return nil, wouldFail("destination trustline for %s is not authorized", p.Asset.StringCanonical())
		}
		room := int64(dstLine.Limit) - int64(dstLine.Balance) - int64(dstLine.Liabilities().Buying)
		if room < int64(p.Amount) {
			return nil, wouldFail("payment would exceed destination trustline limit for %s", p.Asset.StringCanonical())
		}
		dstAfter := dstLine
		dstAfter.Balance += p.Amount
		changes = append(changes, trustlineChangePair(dstLine, dstAfter)...)
	}
	return changes, nil
}

func computeCreateAccountChanges(c xdr.CreateAccountOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, ledgerSeq uint32) (xdr.LedgerEntryChanges, error) {
	srcAccount, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	if _, exists := lookupAccount(before, c.Destination); exists {
		return nil, wouldFail("destination account %s already exists", c.Destination.Address())
	}
	if int64(c.StartingBalance) < 2*processors.BaseReserveStroops {
		return nil, wouldFail("starting balance %d is below the minimum account reserve %d", c.StartingBalance, 2*processors.BaseReserveStroops)
	}
	available := accountSpendableBalance(srcAccount)
	if available < int64(c.StartingBalance) {
		return nil, wouldFail("source account %s has insufficient XLM to fund %d stroops", opSource.Address(), c.StartingBalance)
	}

	srcAfter := srcAccount
	srcAfter.Balance -= c.StartingBalance
	created := xdr.AccountEntry{
		AccountId: c.Destination,
		Balance:   c.StartingBalance,
		// A new account's sequence number starts at ledgerSeq << 32, matching Core.
		SeqNum:     xdr.SequenceNumber(int64(ledgerSeq) << 32),
		Thresholds: xdr.Thresholds{1, 0, 0, 0},
	}
	return append(
		accountChangePair(srcAccount, srcAfter),
		xdr.LedgerEntryChange{
			Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Created: &xdr.LedgerEntry{Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeAccount, Account: &created}},
		},
	), nil
}

func computeChangeTrustChanges(ct xdr.ChangeTrustOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry) (xdr.LedgerEntryChanges, error) {
	asset, _ := changeTrustCreditAsset(ct.Line) // pool shares rejected by classicFootprint
	if opSource.Address() == asset.GetIssuer() {
		return nil, wouldFail("an issuer cannot trust its own asset %s", asset.StringCanonical())
	}
	srcAccount, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}

	line, exists := lookupTrustline(before, opSource, asset)

	// Core's single validity check for an existing line: the new limit must
	// hold the balance plus what open buy offers could still add. A zero limit
	// means deletion, which this covers too, since any balance or buying
	// liability blocks it (selling liabilities never exceed the balance, so
	// they cannot block a deletion on their own).
	if exists && int64(ct.Limit) < int64(line.Balance)+int64(line.Liabilities().Buying) {
		return nil, wouldFail("new limit %d for %s is below the current balance and buying liabilities", ct.Limit, asset.StringCanonical())
	}

	if ct.Limit == 0 {
		if !exists {
			return nil, wouldFail("cannot remove nonexistent trustline for %s", asset.StringCanonical())
		}
		// A trustline deposited into liquidity pools cannot be deleted until
		// those pool shares are redeemed.
		if v1, ok := line.Ext.GetV1(); ok {
			if v2, ok := v1.Ext.GetV2(); ok && v2.LiquidityPoolUseCount != 0 {
				return nil, wouldFail("trustline for %s is in use by %d liquidity pools", asset.StringCanonical(), v2.LiquidityPoolUseCount)
			}
		}
		srcAfter := srcAccount
		dropSubentry(&srcAfter)
		removedKey := trustlineLedgerKey(opSource, asset)
		return append(xdr.LedgerEntryChanges{
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: trustlineLedgerEntry(line)},
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &removedKey},
		}, accountChangePair(srcAccount, srcAfter)...), nil
	}

	// Keeping or creating a line requires the issuer to still exist (trustlines
	// can outlive a merged issuer; Core returns CHANGE_TRUST_NO_ISSUER).
	issuerAccount, ok := lookupAccount(before, xdr.MustAddress(asset.GetIssuer()))
	if !ok {
		return nil, wouldFail("issuer of %s does not exist", asset.StringCanonical())
	}

	if exists {
		after := line
		after.Limit = ct.Limit
		return trustlineChangePair(line, after), nil
	}

	// Creating the trustline: a new subentry, which locks one more base
	// reserve on the source account. Whether the line starts out authorized
	// depends on the issuer's AUTH_REQUIRED flag, and clawback-enabled issuers
	// stamp their trustlines with the clawback flag, matching what Core does.
	srcAfter := srcAccount
	if err := affordSubentry(&srcAfter); err != nil {
		return nil, err
	}
	var flags xdr.Uint32
	if issuerAccount.Flags&xdr.Uint32(xdr.AccountFlagsAuthRequiredFlag) == 0 {
		flags = xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
	}
	if issuerAccount.Flags&xdr.Uint32(xdr.AccountFlagsAuthClawbackEnabledFlag) != 0 {
		flags |= xdr.Uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag)
	}
	created := xdr.TrustLineEntry{
		AccountId: opSource,
		Asset:     asset.ToTrustLineAsset(),
		Balance:   0,
		Limit:     ct.Limit,
		Flags:     flags,
	}
	return append(xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: trustlineLedgerEntry(created)},
	}, accountChangePair(srcAccount, srcAfter)...), nil
}

func computeSetOptionsChanges(so xdr.SetOptionsOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry) (xdr.LedgerEntryChanges, error) {
	account, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	after := account

	if so.InflationDest != nil {
		// The network only accepts an inflation destination that exists.
		if _, ok := lookupAccount(before, *so.InflationDest); !ok {
			return nil, wouldFail("inflation destination account %s does not exist", so.InflationDest.Address())
		}
		dest := *so.InflationDest
		after.InflationDest = &dest
	}
	if so.ClearFlags != nil || so.SetFlags != nil {
		// An AUTH_IMMUTABLE account cannot touch its auth flags
		// (SET_OPTIONS_CANT_CHANGE), and the result must keep AUTH_REVOCABLE
		// whenever AUTH_CLAWBACK_ENABLED is set (AUTH_REVOCABLE_REQUIRED).
		var touched xdr.Uint32
		if so.ClearFlags != nil {
			touched |= *so.ClearFlags
			after.Flags &^= *so.ClearFlags
		}
		if so.SetFlags != nil {
			touched |= *so.SetFlags
			after.Flags |= *so.SetFlags
		}
		if touched&allAccountAuthFlags != 0 && account.Flags&xdr.Uint32(xdr.AccountFlagsAuthImmutableFlag) != 0 {
			return nil, wouldFail("account %s has AUTH_IMMUTABLE set and cannot change flags", opSource.Address())
		}
		if after.Flags&xdr.Uint32(xdr.AccountFlagsAuthClawbackEnabledFlag) != 0 &&
			after.Flags&xdr.Uint32(xdr.AccountFlagsAuthRevocableFlag) == 0 {
			return nil, wouldFail("AUTH_CLAWBACK_ENABLED requires AUTH_REVOCABLE on account %s", opSource.Address())
		}
	}
	if so.MasterWeight != nil {
		after.Thresholds[0] = byte(*so.MasterWeight)
	}
	if so.LowThreshold != nil {
		after.Thresholds[1] = byte(*so.LowThreshold)
	}
	if so.MedThreshold != nil {
		after.Thresholds[2] = byte(*so.MedThreshold)
	}
	if so.HighThreshold != nil {
		after.Thresholds[3] = byte(*so.HighThreshold)
	}
	if so.HomeDomain != nil {
		after.HomeDomain = *so.HomeDomain
	}
	if so.Signer != nil {
		// The master key is not in the signer list; Core rejects adding it
		// (SET_OPTIONS_BAD_SIGNER).
		if so.Signer.Key.Equals(xdr.MustSigner(opSource.Address())) {
			return nil, wouldFail("account %s cannot add its own master key as a signer", opSource.Address())
		}
		exists := false
		for _, s := range account.Signers {
			if s.Key.Equals(so.Signer.Key) {
				exists = true
				break
			}
		}
		if so.Signer.Weight == 0 && !exists {
			return nil, wouldFail("account %s has no signer %s to remove", opSource.Address(), so.Signer.Key.Address())
		}
		if so.Signer.Weight > 0 && !exists && len(account.Signers) >= maxAccountSigners {
			return nil, wouldFail("account %s already has the maximum of %d signers", opSource.Address(), maxAccountSigners)
		}
		applySignerChange(&after, *so.Signer)
		// A new signer is a subentry: it locks one more base reserve; a
		// removed one releases it.
		switch {
		case len(after.Signers) > len(account.Signers):
			if err := affordSubentry(&after); err != nil {
				return nil, err
			}
		case len(after.Signers) < len(account.Signers):
			dropSubentry(&after)
		}
	}

	return accountChangePair(account, after), nil
}

// applySignerChange applies one signer change to the account: a weight of zero
// removes the signer, any other weight adds or updates it. An account carrying
// the v2 extension holds exactly one sponsoring-ID slot per signer and the
// processors index the two slices together, so the slots are rebuilt in
// lockstep, into fresh extension structs because the originals are shared with
// the before entry.
func applySignerChange(account *xdr.AccountEntry, change xdr.Signer) {
	hasV2 := account.Ext.V1 != nil && account.Ext.V1.Ext.V2 != nil
	var oldIDs []xdr.SponsorshipDescriptor
	if hasV2 {
		oldIDs = account.Ext.V1.Ext.V2.SignerSponsoringIDs
	}

	signers := make([]xdr.Signer, 0, len(account.Signers)+1)
	ids := make([]xdr.SponsorshipDescriptor, 0, len(account.Signers)+1)
	found := false
	for i, s := range account.Signers {
		if s.Key.Equals(change.Key) {
			found = true
			if change.Weight > 0 {
				signers = append(signers, change)
				if hasV2 {
					ids = append(ids, oldIDs[i])
				}
			}
			continue
		}
		signers = append(signers, s)
		if hasV2 {
			ids = append(ids, oldIDs[i])
		}
	}
	if !found && change.Weight > 0 {
		signers = append(signers, change)
		// The account pays the new signer's reserve itself, so its slot is
		// unsponsored.
		ids = append(ids, nil)
	}

	account.Signers = signers
	if hasV2 {
		v1 := *account.Ext.V1
		v2 := *v1.Ext.V2
		v2.SignerSponsoringIDs = ids
		v1.Ext.V2 = &v2
		account.Ext.V1 = &v1
	}
}

func computeManageDataChanges(md xdr.ManageDataOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry) (xdr.LedgerEntryChanges, error) {
	srcAccount, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	name := string(md.DataName)
	existing, exists := lookupData(before, opSource, name)

	if md.DataValue == nil {
		if !exists {
			return nil, wouldFail("cannot remove nonexistent data entry %q", name)
		}
		srcAfter := srcAccount
		dropSubentry(&srcAfter)
		removedKey := dataLedgerKey(opSource, name)
		return append(xdr.LedgerEntryChanges{
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: dataLedgerEntry(existing)},
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &removedKey},
		}, accountChangePair(srcAccount, srcAfter)...), nil
	}

	after := xdr.DataEntry{
		AccountId: opSource,
		DataName:  md.DataName,
		DataValue: *md.DataValue,
	}
	if !exists {
		// A new data entry is a subentry: it locks one more base reserve.
		srcAfter := srcAccount
		if err := affordSubentry(&srcAfter); err != nil {
			return nil, err
		}
		return append(xdr.LedgerEntryChanges{
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: dataLedgerEntry(after)},
		}, accountChangePair(srcAccount, srcAfter)...), nil
	}
	return xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: dataLedgerEntry(existing)},
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: dataLedgerEntry(after)},
	}, nil
}

// ---- lookups and entry constructors ----

func accountLedgerKey(id xdr.AccountId) xdr.LedgerKey {
	return xdr.LedgerKey{Type: xdr.LedgerEntryTypeAccount, Account: &xdr.LedgerKeyAccount{AccountId: id}}
}

func trustlineLedgerKey(id xdr.AccountId, asset xdr.Asset) xdr.LedgerKey {
	return xdr.LedgerKey{Type: xdr.LedgerEntryTypeTrustline, TrustLine: &xdr.LedgerKeyTrustLine{
		AccountId: id,
		Asset:     asset.ToTrustLineAsset(),
	}}
}

func dataLedgerKey(id xdr.AccountId, name string) xdr.LedgerKey {
	return xdr.LedgerKey{Type: xdr.LedgerEntryTypeData, Data: &xdr.LedgerKeyData{
		AccountId: id,
		DataName:  xdr.String64(name),
	}}
}

func lookupEntry(before map[string]xdr.LedgerEntry, key xdr.LedgerKey) (xdr.LedgerEntry, bool) {
	b64, err := xdr.MarshalBase64(key)
	if err != nil {
		return xdr.LedgerEntry{}, false
	}
	entry, ok := before[b64]
	return entry, ok
}

// storeAccount writes the account back into the before map, so later reads see
// the updated image (used to charge the fee ahead of the operation).
func storeAccount(before map[string]xdr.LedgerEntry, account xdr.AccountEntry) {
	b64, err := xdr.MarshalBase64(accountLedgerKey(account.AccountId))
	if err != nil {
		return
	}
	entry := before[b64]
	entry.Data = xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeAccount, Account: &account}
	before[b64] = entry
}

func lookupAccount(before map[string]xdr.LedgerEntry, id xdr.AccountId) (xdr.AccountEntry, bool) {
	entry, ok := lookupEntry(before, accountLedgerKey(id))
	if !ok || entry.Data.Type != xdr.LedgerEntryTypeAccount {
		return xdr.AccountEntry{}, false
	}
	return *entry.Data.Account, true
}

func lookupTrustline(before map[string]xdr.LedgerEntry, id xdr.AccountId, asset xdr.Asset) (xdr.TrustLineEntry, bool) {
	entry, ok := lookupEntry(before, trustlineLedgerKey(id, asset))
	if !ok || entry.Data.Type != xdr.LedgerEntryTypeTrustline {
		return xdr.TrustLineEntry{}, false
	}
	return *entry.Data.TrustLine, true
}

func lookupData(before map[string]xdr.LedgerEntry, id xdr.AccountId, name string) (xdr.DataEntry, bool) {
	entry, ok := lookupEntry(before, dataLedgerKey(id, name))
	if !ok || entry.Data.Type != xdr.LedgerEntryTypeData {
		return xdr.DataEntry{}, false
	}
	return *entry.Data.Data, true
}

func accountLedgerEntry(account xdr.AccountEntry) *xdr.LedgerEntry {
	return &xdr.LedgerEntry{Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeAccount, Account: &account}}
}

func trustlineLedgerEntry(line xdr.TrustLineEntry) *xdr.LedgerEntry {
	return &xdr.LedgerEntry{Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeTrustline, TrustLine: &line}}
}

func dataLedgerEntry(data xdr.DataEntry) *xdr.LedgerEntry {
	return &xdr.LedgerEntry{Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeData, Data: &data}}
}

func accountChangePair(beforeEntry, afterEntry xdr.AccountEntry) xdr.LedgerEntryChanges {
	return xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: accountLedgerEntry(beforeEntry)},
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: accountLedgerEntry(afterEntry)},
	}
}

func trustlineChangePair(beforeLine, afterLine xdr.TrustLineEntry) xdr.LedgerEntryChanges {
	return xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: trustlineLedgerEntry(beforeLine)},
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: trustlineLedgerEntry(afterLine)},
	}
}

// accountSpendableBalance is the XLM the account can actually spend: its
// balance minus the locked reserves and the amounts committed to open offers.
func accountSpendableBalance(account xdr.AccountEntry) int64 {
	return int64(account.Balance) - processors.MinimumBalance(account) - int64(account.Liabilities().Selling)
}

// affordSubentry verifies the account can afford the base reserve one more
// subentry locks up and bumps its subentry counter.
func affordSubentry(account *xdr.AccountEntry) error {
	if accountSpendableBalance(*account) < processors.BaseReserveStroops {
		return wouldFail("account %s cannot afford the base reserve for a new subentry", account.AccountId.Address())
	}
	account.NumSubEntries++
	return nil
}

// dropSubentry decrements the subentry counter, mirroring the reserve released
// by a removed trustline, signer, or data entry.
func dropSubentry(account *xdr.AccountEntry) {
	if account.NumSubEntries > 0 {
		account.NumSubEntries--
	}
}

// changeTrustCreditAsset unwraps a ChangeTrustAsset into a plain credit asset;
// pool-share lines return false.
func changeTrustCreditAsset(line xdr.ChangeTrustAsset) (xdr.Asset, bool) {
	switch line.Type {
	case xdr.AssetTypeAssetTypeCreditAlphanum4, xdr.AssetTypeAssetTypeCreditAlphanum12:
		return line.ToAsset(), true
	default:
		return xdr.Asset{}, false
	}
}

// wouldFail wraps a precondition failure as ErrSimulationFailed: the derivation
// determined the network would reject this transaction, so instead of a preview
// the client gets the reason it would fail.
func wouldFail(format string, args ...any) error {
	return fmt.Errorf("%w: transaction would fail: %s", ErrSimulationFailed, fmt.Sprintf(format, args...))
}
