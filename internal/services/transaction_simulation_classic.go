package services

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// baseReserveStroops is the network base reserve (0.5 XLM). The real value
// lives in the ledger header, but it has not changed since 2015 and previews
// accept approximate reserve math, so a constant is good enough here.
const baseReserveStroops = 5_000_000

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

	// The network rejects a transaction whose source cannot cover the fee bid,
	// before the operation ever runs.
	fee := int64(envelope.Fee())
	feeSource, ok := lookupAccount(before, txSource)
	if !ok {
		return ingest.LedgerTransaction{}, 0, wouldFail("transaction source account %s does not exist", txSource.Address())
	}
	if accountSpendableBalance(feeSource) < fee {
		return ingest.LedgerTransaction{}, 0, wouldFail("transaction source account %s cannot cover the %d stroop fee", txSource.Address(), fee)
	}

	// The fee is charged before the operation applies, so when the operation
	// spends from the fee-paying account its spendable balance is reduced by
	// the fee. Operations with their own source account do not pay it.
	var opSourceFee int64
	if opSource.Equals(txSource) {
		opSourceFee = fee
	}

	changes, err := computeClassicChanges(op, opSource, before, latestLedger, opSourceFee)
	if err != nil {
		return ingest.LedgerTransaction{}, 0, err
	}

	opResults, err := successOperationResults(envelope)
	if err != nil {
		return ingest.LedgerTransaction{}, 0, err
	}
	// The fee is an estimate: the declared bid is what the network charges a
	// classic transaction outside surge pricing.
	tx := newSimulatedLedgerTransaction(envelope, latestLedger, int64(envelope.Fee()), changes, nil, opResults)
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
	case xdr.OperationTypeChangeTrust:
		if op.Body.ChangeTrustOp.Limit < 0 {
			return wouldFail("trustline limit cannot be negative, got %d", op.Body.ChangeTrustOp.Limit)
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
	default:
		// createAccount's starting balance is validated against the minimum
		// reserve in its handler; manageData's field bounds are enforced by
		// XDR decoding (String64 / opaque<64>).
	}
	return nil
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
// that would not succeed. opSourceFee is the transaction fee the operation's
// source also pays (zero when a different account pays it); XLM-spending
// handlers subtract it from the spendable balance, matching the network, which
// charges the fee before the operation applies.
func computeClassicChanges(op xdr.Operation, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, ledgerSeq uint32, opSourceFee int64) (xdr.LedgerEntryChanges, error) {
	switch op.Body.Type {
	case xdr.OperationTypePayment:
		return computePaymentChanges(*op.Body.PaymentOp, opSource, before, opSourceFee)
	case xdr.OperationTypeCreateAccount:
		return computeCreateAccountChanges(*op.Body.CreateAccountOp, opSource, before, ledgerSeq, opSourceFee)
	case xdr.OperationTypeChangeTrust:
		return computeChangeTrustChanges(*op.Body.ChangeTrustOp, opSource, before, opSourceFee)
	case xdr.OperationTypeSetOptions:
		return computeSetOptionsChanges(*op.Body.SetOptionsOp, opSource, before, opSourceFee)
	case xdr.OperationTypeManageData:
		return computeManageDataChanges(*op.Body.ManageDataOp, opSource, before, opSourceFee)
	default:
		// classicFootprint already rejected unsupported types; this is a guard
		// against the two switches drifting apart.
		return nil, fmt.Errorf("%w: classic operation type %s is not supported", ErrUnsupportedTransaction, op.Body.Type)
	}
}

func computePaymentChanges(p xdr.PaymentOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, opSourceFee int64) (xdr.LedgerEntryChanges, error) {
	dst := p.Destination.ToAccountId()
	srcAccount, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	if _, ok := lookupAccount(before, dst); !ok {
		return nil, wouldFail("destination account %s does not exist", dst.Address())
	}

	if p.Asset.Type == xdr.AssetTypeAssetTypeNative {
		if opSource.Equals(dst) {
			// A self-payment moves nothing; emit no entry changes. The
			// token-transfer processor still derives the debit/credit pair
			// from the operation itself, matching history.
			return xdr.LedgerEntryChanges{}, nil
		}
		dstAccount, _ := lookupAccount(before, dst)
		available := int64(srcAccount.Balance) - accountMinBalance(srcAccount) - accountSellingLiabilities(srcAccount) - opSourceFee
		if available < int64(p.Amount) {
			return nil, wouldFail("source account %s has insufficient XLM: available %d stroops, sending %d", opSource.Address(), available, p.Amount)
		}
		srcAfter := cloneAccountEntry(srcAccount)
		srcAfter.Balance -= p.Amount
		dstAfter := cloneAccountEntry(dstAccount)
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
			return nil, wouldFail("source account %s holds no trustline for %s", opSource.Address(), assetString(p.Asset))
		}
		if srcLine.Flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag) == 0 {
			return nil, wouldFail("source trustline for %s is not authorized", assetString(p.Asset))
		}
		available := int64(srcLine.Balance) - trustlineSellingLiabilities(srcLine)
		if available < int64(p.Amount) {
			return nil, wouldFail("source trustline for %s has insufficient balance: available %d, sending %d", assetString(p.Asset), available, p.Amount)
		}
		srcAfter := srcLine
		srcAfter.Balance -= p.Amount
		changes = append(changes, trustlineChangePair(srcLine, srcAfter)...)
	}
	if dst.Address() != issuer {
		dstLine, ok := lookupTrustline(before, dst, p.Asset)
		if !ok {
			return nil, wouldFail("destination account %s holds no trustline for %s", dst.Address(), assetString(p.Asset))
		}
		if dstLine.Flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag) == 0 {
			return nil, wouldFail("destination trustline for %s is not authorized", assetString(p.Asset))
		}
		room := int64(dstLine.Limit) - int64(dstLine.Balance) - trustlineBuyingLiabilities(dstLine)
		if room < int64(p.Amount) {
			return nil, wouldFail("payment would exceed destination trustline limit for %s", assetString(p.Asset))
		}
		dstAfter := dstLine
		dstAfter.Balance += p.Amount
		changes = append(changes, trustlineChangePair(dstLine, dstAfter)...)
	}
	return changes, nil
}

func computeCreateAccountChanges(c xdr.CreateAccountOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, ledgerSeq uint32, opSourceFee int64) (xdr.LedgerEntryChanges, error) {
	srcAccount, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	if _, exists := lookupAccount(before, c.Destination); exists {
		return nil, wouldFail("destination account %s already exists", c.Destination.Address())
	}
	if int64(c.StartingBalance) < 2*baseReserveStroops {
		return nil, wouldFail("starting balance %d is below the minimum account reserve %d", c.StartingBalance, 2*baseReserveStroops)
	}
	available := int64(srcAccount.Balance) - accountMinBalance(srcAccount) - accountSellingLiabilities(srcAccount) - opSourceFee
	if available < int64(c.StartingBalance) {
		return nil, wouldFail("source account %s has insufficient XLM to fund %d stroops", opSource.Address(), c.StartingBalance)
	}

	srcAfter := cloneAccountEntry(srcAccount)
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

func computeChangeTrustChanges(ct xdr.ChangeTrustOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, opSourceFee int64) (xdr.LedgerEntryChanges, error) {
	asset, _ := changeTrustCreditAsset(ct.Line) // pool shares rejected by classicFootprint
	if opSource.Address() == asset.GetIssuer() {
		return nil, wouldFail("an issuer cannot trust its own asset %s", assetString(asset))
	}
	srcAccount, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}

	line, exists := lookupTrustline(before, opSource, asset)

	if ct.Limit == 0 {
		// Deleting the trustline. Open offers hold liabilities against it, so
		// any liability blocks deletion just like a remaining balance.
		if !exists {
			return nil, wouldFail("cannot remove nonexistent trustline for %s", assetString(asset))
		}
		if line.Balance != 0 {
			return nil, wouldFail("cannot remove trustline for %s with outstanding balance %d", assetString(asset), line.Balance)
		}
		if trustlineBuyingLiabilities(line) != 0 || trustlineSellingLiabilities(line) != 0 {
			return nil, wouldFail("cannot remove trustline for %s with open offer liabilities", assetString(asset))
		}
		removedKey := trustlineLedgerKey(opSource, asset)
		return append(xdr.LedgerEntryChanges{
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: trustlineLedgerEntry(line)},
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &removedKey},
		}, accountChangePair(srcAccount, dropSubentry(srcAccount))...), nil
	}

	if exists {
		if int64(ct.Limit) < int64(line.Balance)+trustlineBuyingLiabilities(line) {
			return nil, wouldFail("new limit %d for %s is below the current balance", ct.Limit, assetString(asset))
		}
		after := line
		after.Limit = ct.Limit
		return trustlineChangePair(line, after), nil
	}

	// Creating the trustline: a new subentry, which locks one more base
	// reserve on the source account. Whether the line starts out authorized
	// depends on the issuer's AUTH_REQUIRED flag, and clawback-enabled issuers
	// stamp their trustlines with the clawback flag, matching what Core does.
	issuerAccount, ok := lookupAccount(before, xdr.MustAddress(asset.GetIssuer()))
	if !ok {
		return nil, wouldFail("issuer of %s does not exist", assetString(asset))
	}
	srcAfter, err := affordSubentry(srcAccount, opSourceFee)
	if err != nil {
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

func computeSetOptionsChanges(so xdr.SetOptionsOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, opSourceFee int64) (xdr.LedgerEntryChanges, error) {
	account, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	after := cloneAccountEntry(account)

	if so.InflationDest != nil {
		// The network only accepts an inflation destination that exists.
		if _, ok := lookupAccount(before, *so.InflationDest); !ok {
			return nil, wouldFail("inflation destination account %s does not exist", so.InflationDest.Address())
		}
		dest := *so.InflationDest
		after.InflationDest = &dest
	}
	if so.ClearFlags != nil {
		after.Flags &^= *so.ClearFlags
	}
	if so.SetFlags != nil {
		after.Flags |= *so.SetFlags
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
		after.Signers = applySignerChange(after.Signers, *so.Signer)
		// A new signer is a subentry: it locks one more base reserve; a
		// removed one releases it.
		switch {
		case len(after.Signers) > len(account.Signers):
			if accountSpendableBalance(account)-opSourceFee < baseReserveStroops {
				return nil, wouldFail("account %s cannot afford the base reserve for a new signer", opSource.Address())
			}
			after.NumSubEntries++
		case len(after.Signers) < len(account.Signers):
			if after.NumSubEntries > 0 {
				after.NumSubEntries--
			}
		}
	}

	return accountChangePair(account, after), nil
}

// applySignerChange returns the signer list after applying one change: a weight
// of zero removes the signer, any other weight adds or updates it.
func applySignerChange(signers []xdr.Signer, change xdr.Signer) []xdr.Signer {
	out := make([]xdr.Signer, 0, len(signers)+1)
	found := false
	for _, s := range signers {
		if s.Key.Equals(change.Key) {
			found = true
			if change.Weight > 0 {
				out = append(out, change)
			}
			continue
		}
		out = append(out, s)
	}
	if !found && change.Weight > 0 {
		out = append(out, change)
	}
	return out
}

func computeManageDataChanges(md xdr.ManageDataOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, opSourceFee int64) (xdr.LedgerEntryChanges, error) {
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
		removedKey := dataLedgerKey(opSource, name)
		return append(xdr.LedgerEntryChanges{
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: dataLedgerEntry(existing)},
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &removedKey},
		}, accountChangePair(srcAccount, dropSubentry(srcAccount))...), nil
	}

	after := xdr.DataEntry{
		AccountId: opSource,
		DataName:  md.DataName,
		DataValue: *md.DataValue,
	}
	if !exists {
		// A new data entry is a subentry: it locks one more base reserve.
		srcAfter, err := affordSubentry(srcAccount, opSourceFee)
		if err != nil {
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

// cloneAccountEntry copies an account entry so the copy can be modified without
// also changing the original: the entry holds a pointer and a slice (inflation
// destination and signers) that a plain struct copy would share.
func cloneAccountEntry(account xdr.AccountEntry) xdr.AccountEntry {
	out := account
	if account.InflationDest != nil {
		dest := *account.InflationDest
		out.InflationDest = &dest
	}
	out.Signers = append([]xdr.Signer(nil), account.Signers...)
	return out
}

// accountSpendableBalance is the XLM the account can actually spend: its
// balance minus the locked reserves and the amounts committed to open offers.
func accountSpendableBalance(account xdr.AccountEntry) int64 {
	return int64(account.Balance) - accountMinBalance(account) - accountSellingLiabilities(account)
}

// affordSubentry verifies the account can afford the base reserve one more
// subentry locks up (after paying opSourceFee when it is also the fee payer)
// and returns the account with its subentry counter bumped.
func affordSubentry(account xdr.AccountEntry, opSourceFee int64) (xdr.AccountEntry, error) {
	if accountSpendableBalance(account)-opSourceFee < baseReserveStroops {
		return xdr.AccountEntry{}, wouldFail("account %s cannot afford the base reserve for a new subentry", account.AccountId.Address())
	}
	after := cloneAccountEntry(account)
	after.NumSubEntries++
	return after, nil
}

// dropSubentry returns the account with its subentry counter decremented,
// mirroring the reserve released by a removed trustline, signer, or data entry.
func dropSubentry(account xdr.AccountEntry) xdr.AccountEntry {
	after := cloneAccountEntry(account)
	if after.NumSubEntries > 0 {
		after.NumSubEntries--
	}
	return after
}

// accountMinBalance is the XLM the account must keep locked as reserves: two
// base-reserve slots for the account itself plus one per subentry, adjusted by
// the sponsorship counters.
func accountMinBalance(account xdr.AccountEntry) int64 {
	subEntries := int64(account.NumSubEntries)
	sponsoring, sponsored := int64(0), int64(0)
	if v1, ok := account.Ext.GetV1(); ok {
		if v2, ok := v1.Ext.GetV2(); ok {
			sponsoring = int64(v2.NumSponsoring)
			sponsored = int64(v2.NumSponsored)
		}
	}
	return (2 + subEntries + sponsoring - sponsored) * baseReserveStroops
}

func accountSellingLiabilities(account xdr.AccountEntry) int64 {
	if v1, ok := account.Ext.GetV1(); ok {
		return int64(v1.Liabilities.Selling)
	}
	return 0
}

func trustlineSellingLiabilities(line xdr.TrustLineEntry) int64 {
	if v1, ok := line.Ext.GetV1(); ok {
		return int64(v1.Liabilities.Selling)
	}
	return 0
}

func trustlineBuyingLiabilities(line xdr.TrustLineEntry) int64 {
	if v1, ok := line.Ext.GetV1(); ok {
		return int64(v1.Liabilities.Buying)
	}
	return 0
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

func assetString(asset xdr.Asset) string {
	return asset.StringCanonical()
}

// wouldFail wraps a precondition failure as ErrSimulationFailed: the derivation
// determined the network would reject this transaction, so instead of a preview
// the client gets the reason it would fail.
func wouldFail(format string, args ...any) error {
	return fmt.Errorf("%w: transaction would fail: %s", ErrSimulationFailed, fmt.Sprintf(format, args...))
}
