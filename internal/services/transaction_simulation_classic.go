package services

import (
	"crypto/sha256"
	"fmt"
	"math"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
)

// maxAccountSigners is the ledger's cap on extra signers per account (the
// master key is not in the list): AccountEntry declares `Signer signers<20>`,
// and Core rejects the 21st with SET_OPTIONS_TOO_MANY_SIGNERS.
const maxAccountSigners = 20

// maxAccountSubEntries is the protocol's cap on subentries per account
// (trustlines, data entries, signers, offers).
const maxAccountSubEntries = 1000

// printableASCII reports whether every byte is printable ASCII (0x20-0x7E),
// the character set Core accepts for data entry names and home domains.
func printableASCII(s string) bool {
	for _, c := range []byte(s) {
		if c <= 0x1f || c >= 0x7f {
			return false
		}
	}
	return true
}

// ledgerTransactionFromClassic builds the simulated ledger transaction for a
// classic transaction. RPC cannot simulate classic operations, so this path
// derives the outcome itself in three steps: list the ledger entries the
// operation touches (classicFootprint), fetch their current state from RPC
// (fetchLedgerEntries), and apply the operation's stated effect to those
// entries (computeClassicChanges). The resulting before/after entries feed
// the same synthesis and processors as the Soroban path.
func (s *transactionSimulationService) ledgerTransactionFromClassic(envelope xdr.TransactionEnvelope) (ingest.LedgerTransaction, uint32, error) {
	ops := envelope.Operations()
	// For a fee-bump envelope the operations come from the inner transaction
	// but the fee is paid by the wrapper's fee source; FeeAccount resolves to
	// the plain source for ordinary envelopes.
	feeAccount := envelope.FeeAccount().ToAccountId()

	// 1. Validate each operation and fetch every ledger entry the transaction
	//    touches, always including the fee-paying account.
	keys := []xdr.LedgerKey{accountLedgerKey(feeAccount)}
	for _, op := range ops {
		if err := validateClassicOperation(op); err != nil {
			return ingest.LedgerTransaction{}, 0, err
		}
		opKeys, err := classicFootprint(op, classicOperationSource(envelope, op))
		if err != nil {
			return ingest.LedgerTransaction{}, 0, err
		}
		keys = append(keys, opKeys...)
	}
	// A claim's credited asset lives inside the claimable-balance entry, so the
	// claimant's trustline key is only knowable after the first fetch. Both
	// rounds must observe the same ledger, or the working state would combine
	// entries that never coexisted; when a ledger closes in between, refetch.
	var working map[string]xdr.LedgerEntry
	var latestLedger uint32
	for attempt := 0; ; attempt++ {
		var err error
		working, latestLedger, err = s.fetchLedgerEntries(keys)
		if err != nil {
			return ingest.LedgerTransaction{}, 0, err
		}
		extraKeys := claimantTrustlineKeys(envelope, working)
		if len(extraKeys) == 0 {
			break
		}
		extra, extraLedger, err := s.fetchLedgerEntries(extraKeys)
		if err != nil {
			return ingest.LedgerTransaction{}, 0, err
		}
		if extraLedger == latestLedger {
			for key, entry := range extra {
				if _, dup := working[key]; !dup {
					working[key] = entry
				}
			}
			break
		}
		if attempt == 2 {
			return ingest.LedgerTransaction{}, 0, fmt.Errorf("ledger kept advancing while fetching the footprint; retry the simulation")
		}
	}

	// 2. Reject what the network would reject up front: a bid below the base
	//    fee per operation, or a fee source that cannot cover the full bid.
	feeBid := envelopeFeeBid(envelope)
	feeCharged := int64(len(ops)) * baseFeeStroops
	if envelope.Type == xdr.EnvelopeTypeEnvelopeTypeTxFeeBump {
		// Core charges a fee-bump as one extra operation.
		feeCharged += baseFeeStroops
	}
	if feeBid < feeCharged {
		return ingest.LedgerTransaction{}, 0, wouldFail("fee bid %d is below the %d stroop minimum for this transaction", feeBid, feeCharged)
	}
	feeSource, ok := lookupAccount(working, feeAccount)
	if !ok {
		return ingest.LedgerTransaction{}, 0, wouldFail("fee-paying account %s does not exist", feeAccount.Address())
	}
	if accountSpendableBalance(feeSource) < feeBid {
		return ingest.LedgerTransaction{}, 0, wouldFail("fee-paying account %s cannot cover the %d stroop fee bid", feeAccount.Address(), feeBid)
	}

	// Charge the fee before any operation runs: outside surge pricing Core
	// charges exactly the base fee per operation, and the bid only caps it.
	// The subtraction is internal bookkeeping, not an emitted change; the
	// preview's fee row is derived from Result.FeeCharged, so emitting it too
	// would double-count the fee.
	feeSourceAfter := feeSource
	feeSourceAfter.Balance -= xdr.Int64(feeCharged)
	if err := storeWorkingEntry(working, accountLedgerEntry(feeSourceAfter)); err != nil {
		return ingest.LedgerTransaction{}, 0, fmt.Errorf("charging fee to working state: %w", err)
	}

	// 3. Apply the operations in order against the evolving working state; any
	//    failure aborts the whole preview. The op's result is built before its
	//    changes are applied, because some results carry pre-change state (an
	//    account merge's result records the balance being transferred).
	opMetas := make([]xdr.OperationMetaV2, len(ops))
	opResults := make([]xdr.OperationResult, len(ops))
	now := time.Now()
	for i, op := range ops {
		opSource := classicOperationSource(envelope, op)
		env := classicOpEnv{envelope: envelope, opIndex: i, now: now}
		changes, err := computeClassicChanges(op, opSource, working, latestLedger, env)
		if err != nil {
			return ingest.LedgerTransaction{}, 0, fmt.Errorf("operation %d: %w", i+1, err)
		}
		opResults[i], err = classicOperationResult(op, opSource, working, env)
		if err != nil {
			return ingest.LedgerTransaction{}, 0, fmt.Errorf("operation %d: %w", i+1, err)
		}
		opMetas[i] = xdr.OperationMetaV2{Changes: changes}
		if err := applyChangesToWorkingState(working, changes); err != nil {
			return ingest.LedgerTransaction{}, 0, fmt.Errorf("applying operation %d changes: %w", i+1, err)
		}
	}

	// 4. Assemble the ledger transaction the processors will read.
	tx := newSimulatedLedgerTransaction(envelope, latestLedger, feeCharged, opMetas, &opResults)
	return tx, latestLedger, nil
}

// applyChangesToWorkingState folds an operation's emitted entry changes back
// into the working state, so the next operation in the transaction sees them.
func applyChangesToWorkingState(working map[string]xdr.LedgerEntry, changes xdr.LedgerEntryChanges) error {
	for _, change := range changes {
		switch change.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			// The snapshot of how an entry looked before; nothing to apply.
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			if err := storeWorkingEntry(working, change.Created); err != nil {
				return err
			}
		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			if err := storeWorkingEntry(working, change.Updated); err != nil {
				return err
			}
		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			b64, err := xdr.MarshalBase64(*change.Removed)
			if err != nil {
				return fmt.Errorf("encoding removed ledger key: %w", err)
			}
			delete(working, b64)
		case xdr.LedgerEntryChangeTypeLedgerEntryRestored:
			// Never produced by the classic handlers.
		}
	}
	return nil
}

// storeWorkingEntry saves an entry into the working state, keyed the same way
// fetchLedgerEntries keys them, replacing any previous version of that entry.
func storeWorkingEntry(working map[string]xdr.LedgerEntry, entry *xdr.LedgerEntry) error {
	key, err := entry.LedgerKey()
	if err != nil {
		return fmt.Errorf("deriving ledger key: %w", err)
	}
	b64, err := xdr.MarshalBase64(key)
	if err != nil {
		return fmt.Errorf("encoding ledger key: %w", err)
	}
	working[b64] = *entry
	return nil
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
		// An empty home domain is valid and clears it.
		if so.HomeDomain != nil && !printableASCII(string(*so.HomeDomain)) {
			return wouldFail("home domain contains non-printable characters")
		}
	case xdr.OperationTypeManageData:
		name := string(op.Body.ManageDataOp.DataName)
		if len(name) == 0 {
			return wouldFail("data entry name cannot be empty")
		}
		if !printableASCII(name) {
			return wouldFail("data entry name contains non-printable characters")
		}
	case xdr.OperationTypeClawback:
		if op.Body.ClawbackOp.Amount <= 0 {
			return wouldFail("clawback amount must be positive, got %d", op.Body.ClawbackOp.Amount)
		}
	case xdr.OperationTypeSetTrustLineFlags:
		st := op.Body.SetTrustLineFlagsOp
		validFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag | xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag | xdr.TrustLineFlagsTrustlineClawbackEnabledFlag)
		if st.SetFlags&st.ClearFlags != 0 {
			return wouldFail("a trustline flag cannot be both set and cleared in one operation")
		}
		if st.SetFlags&^validFlags != 0 || st.ClearFlags&^validFlags != 0 {
			return wouldFail("unknown trustline flag bits")
		}
		if st.SetFlags&xdr.Uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag) != 0 {
			return wouldFail("the clawback-enabled trustline flag can only be cleared, never set")
		}
	case xdr.OperationTypeAllowTrust:
		if op.Body.AllowTrustOp.Authorize > xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag) {
			return wouldFail("allowTrust authorize value %d is invalid", op.Body.AllowTrustOp.Authorize)
		}
	case xdr.OperationTypeBumpSequence:
		if op.Body.BumpSequenceOp.BumpTo < 0 {
			return wouldFail("bumpTo cannot be negative, got %d", op.Body.BumpSequenceOp.BumpTo)
		}
	case xdr.OperationTypeCreateClaimableBalance:
		cb := op.Body.CreateClaimableBalanceOp
		if cb.Amount <= 0 {
			return wouldFail("claimable balance amount must be positive, got %d", cb.Amount)
		}
		if len(cb.Claimants) == 0 {
			return wouldFail("a claimable balance needs at least one claimant")
		}
		if !creditAssetCodeValid(cb.Asset) {
			return wouldFail("claimable balance asset code is malformed")
		}
		seen := map[string]bool{}
		for _, claimant := range cb.Claimants {
			v0 := claimant.MustV0()
			dest := v0.Destination.Address()
			if seen[dest] {
				return wouldFail("claimant %s is listed twice", dest)
			}
			seen[dest] = true
			if err := validateClaimPredicate(v0.Predicate, 1); err != nil {
				return err
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

	case xdr.OperationTypeSetTrustLineFlags:
		st := op.Body.SetTrustLineFlagsOp
		return []xdr.LedgerKey{
			accountLedgerKey(opSource),
			trustlineLedgerKey(st.Trustor, st.Asset),
		}, nil

	case xdr.OperationTypeAllowTrust:
		at := op.Body.AllowTrustOp
		return []xdr.LedgerKey{
			accountLedgerKey(opSource),
			trustlineLedgerKey(at.Trustor, at.Asset.ToAsset(opSource)),
		}, nil

	case xdr.OperationTypeClawback:
		cb := op.Body.ClawbackOp
		return []xdr.LedgerKey{
			accountLedgerKey(opSource),
			trustlineLedgerKey(cb.From.ToAccountId(), cb.Asset),
		}, nil

	case xdr.OperationTypeBumpSequence:
		return []xdr.LedgerKey{accountLedgerKey(opSource)}, nil

	case xdr.OperationTypeAccountMerge:
		return []xdr.LedgerKey{
			accountLedgerKey(opSource),
			accountLedgerKey(op.Body.MustDestination().ToAccountId()),
		}, nil

	case xdr.OperationTypeCreateClaimableBalance:
		cb := op.Body.CreateClaimableBalanceOp
		keys := []xdr.LedgerKey{accountLedgerKey(opSource)}
		if cb.Asset.Type != xdr.AssetTypeAssetTypeNative && opSource.Address() != cb.Asset.GetIssuer() {
			keys = append(keys, trustlineLedgerKey(opSource, cb.Asset))
		}
		return keys, nil

	case xdr.OperationTypeClaimClaimableBalance:
		// The claimant's trustline for the escrowed asset is fetched in a
		// second round, once the balance entry reveals which asset that is.
		return []xdr.LedgerKey{
			accountLedgerKey(opSource),
			claimableBalanceLedgerKey(op.Body.ClaimClaimableBalanceOp.BalanceId),
		}, nil

	case xdr.OperationTypeClawbackClaimableBalance:
		return []xdr.LedgerKey{
			accountLedgerKey(opSource),
			claimableBalanceLedgerKey(op.Body.ClawbackClaimableBalanceOp.BalanceId),
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

	// getLedgerEntries accepts at most rpcLedgerEntryBatchSize keys per call
	// (a 100-operation transaction can legitimately need more), so fetch in
	// batches. Every batch must observe the same ledger, or the combined
	// entries would describe a state that never existed; if a ledger closes
	// between batches, retry the whole fetch against the new ledger.
	const maxSnapshotAttempts = 3
	for attempt := 1; ; attempt++ {
		entries := make(map[string]xdr.LedgerEntry, len(encoded))
		var latestLedger uint32
		consistent := true
		for start := 0; start < len(encoded); start += rpcLedgerEntryBatchSize {
			end := min(start+rpcLedgerEntryBatchSize, len(encoded))
			result, err := s.rpcService.GetLedgerEntries(encoded[start:end])
			if err != nil {
				return nil, 0, fmt.Errorf("fetching ledger entries via RPC: %w", err)
			}
			if latestLedger == 0 {
				latestLedger = result.LatestLedger
			} else if result.LatestLedger != latestLedger {
				consistent = false
				break
			}
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
		}
		if consistent {
			return entries, latestLedger, nil
		}
		if attempt == maxSnapshotAttempts {
			return nil, 0, fmt.Errorf("ledger advanced during the footprint fetch %d times in a row", attempt)
		}
	}
}

// computeClassicChanges applies the operation's stated effect to the fetched
// before-entries and returns before/after change pairs in the same shape a real
// ledger's transaction meta carries, so the existing processors can read them
// unchanged. When the current state means the network would reject the
// operation (for example paying more than the balance), it returns
// ErrSimulationFailed instead, so a preview is never shown for a transaction
// that would not succeed. The fee has already been debited from the before
// image, so handlers see the balance the operation would actually run against.
func computeClassicChanges(op xdr.Operation, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, ledgerSeq uint32, env classicOpEnv) (xdr.LedgerEntryChanges, error) {
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
	case xdr.OperationTypeSetTrustLineFlags:
		st := *op.Body.SetTrustLineFlagsOp
		return computeTrustlineFlagChanges(opSource, st.Trustor, st.Asset, st.SetFlags, st.ClearFlags, before)
	case xdr.OperationTypeAllowTrust:
		return computeAllowTrustChanges(*op.Body.AllowTrustOp, opSource, before)
	case xdr.OperationTypeClawback:
		return computeClawbackChanges(*op.Body.ClawbackOp, opSource, before)
	case xdr.OperationTypeBumpSequence:
		return computeBumpSequenceChanges(*op.Body.BumpSequenceOp, opSource, before)
	case xdr.OperationTypeAccountMerge:
		return computeAccountMergeChanges(op.Body.MustDestination().ToAccountId(), opSource, before, ledgerSeq)
	case xdr.OperationTypeCreateClaimableBalance:
		id, err := claimableBalanceID(env.envelope, env.opIndex)
		if err != nil {
			return nil, err
		}
		return computeCreateClaimableBalanceChanges(*op.Body.CreateClaimableBalanceOp, opSource, before, id, env.now)
	case xdr.OperationTypeClaimClaimableBalance:
		return computeClaimClaimableBalanceChanges(*op.Body.ClaimClaimableBalanceOp, opSource, before, env.now)
	case xdr.OperationTypeClawbackClaimableBalance:
		return computeClawbackClaimableBalanceChanges(*op.Body.ClawbackClaimableBalanceOp, opSource, before)
	default:
		// classicFootprint already rejected unsupported types; this is a guard
		// against the two switches drifting apart.
		return nil, fmt.Errorf("%w: classic operation type %s is not supported", ErrUnsupportedTransaction, op.Body.Type)
	}
}

// classicOpEnv carries the transaction-level context an operation handler may
// need beyond the operation itself: the envelope and operation index identify
// a created claimable balance, and now anchors time-predicate evaluation.
type classicOpEnv struct {
	envelope xdr.TransactionEnvelope
	opIndex  int
	now      time.Time
}

// classicOperationResult builds the operation's "succeeded" result. The
// processors read results by operation type (see codes.go and the SDK's token
// transfer processor), so every supported classic operation needs its result
// arm populated or they fail on the empty union. Adding an operation here is
// part of the same checklist as classicFootprint and computeClassicChanges.
//
// working is the state BEFORE the operation applies: some results carry state
// payloads the processors read, like the balance an account merge transfers.
func classicOperationResult(op xdr.Operation, opSource xdr.AccountId, working map[string]xdr.LedgerEntry, env classicOpEnv) (xdr.OperationResult, error) {
	tr := xdr.OperationResultTr{Type: op.Body.Type}
	switch op.Body.Type {
	case xdr.OperationTypeCreateClaimableBalance:
		// The SDK's token transfer processor reads the created balance's ID
		// from this result, the same place the real network reports it.
		id, err := claimableBalanceID(env.envelope, env.opIndex)
		if err != nil {
			return xdr.OperationResult{}, err
		}
		tr.CreateClaimableBalanceResult = &xdr.CreateClaimableBalanceResult{
			Code:      xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceSuccess,
			BalanceId: &id,
		}
	case xdr.OperationTypeClaimClaimableBalance:
		tr.ClaimClaimableBalanceResult = &xdr.ClaimClaimableBalanceResult{Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess}
	case xdr.OperationTypeClawbackClaimableBalance:
		tr.ClawbackClaimableBalanceResult = &xdr.ClawbackClaimableBalanceResult{Code: xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceSuccess}
	case xdr.OperationTypeAccountMerge:
		source, ok := lookupAccount(working, opSource)
		if !ok {
			return xdr.OperationResult{}, fmt.Errorf("building accountMerge result: source account %s not in working state", opSource.Address())
		}
		balance := source.Balance
		tr.AccountMergeResult = &xdr.AccountMergeResult{
			Code:                 xdr.AccountMergeResultCodeAccountMergeSuccess,
			SourceAccountBalance: &balance,
		}
	case xdr.OperationTypePayment:
		tr.PaymentResult = &xdr.PaymentResult{Code: xdr.PaymentResultCodePaymentSuccess}
	case xdr.OperationTypeCreateAccount:
		tr.CreateAccountResult = &xdr.CreateAccountResult{Code: xdr.CreateAccountResultCodeCreateAccountSuccess}
	case xdr.OperationTypeChangeTrust:
		tr.ChangeTrustResult = &xdr.ChangeTrustResult{Code: xdr.ChangeTrustResultCodeChangeTrustSuccess}
	case xdr.OperationTypeSetOptions:
		tr.SetOptionsResult = &xdr.SetOptionsResult{Code: xdr.SetOptionsResultCodeSetOptionsSuccess}
	case xdr.OperationTypeManageData:
		tr.ManageDataResult = &xdr.ManageDataResult{Code: xdr.ManageDataResultCodeManageDataSuccess}
	case xdr.OperationTypeSetTrustLineFlags:
		tr.SetTrustLineFlagsResult = &xdr.SetTrustLineFlagsResult{Code: xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsSuccess}
	case xdr.OperationTypeAllowTrust:
		tr.AllowTrustResult = &xdr.AllowTrustResult{Code: xdr.AllowTrustResultCodeAllowTrustSuccess}
	case xdr.OperationTypeClawback:
		tr.ClawbackResult = &xdr.ClawbackResult{Code: xdr.ClawbackResultCodeClawbackSuccess}
	case xdr.OperationTypeBumpSequence:
		tr.BumpSeqResult = &xdr.BumpSequenceResult{Code: xdr.BumpSequenceResultCodeBumpSequenceSuccess}
	default:
		return xdr.OperationResult{}, fmt.Errorf("%w: operation type %s", ErrUnsupportedTransaction, op.Body.Type)
	}
	return xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr}, nil
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
			// The network applies a payment as a credit followed by a debit,
			// so a payment to self succeeds even when the amount exceeds the
			// balance, and moves nothing. It emits no entry changes; emitting
			// a debit and a credit built from the same snapshot would corrupt
			// the working state. The token-transfer processor still derives
			// the debit/credit rows from the operation itself, matching
			// history.
			if int64(p.Amount) > math.MaxInt64-int64(srcAccount.Balance)-int64(srcAccount.Liabilities().Buying) {
				return nil, wouldFail("payment would exceed the maximum XLM balance of %s", opSource.Address())
			}
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
	if opSource.Equals(dst) {
		// Same credit-then-debit rule as the native self-payment above: the
		// balance never limits a payment to self, only the credit side (the
		// trustline's limit) can. The issuer paying itself has no trustline
		// at all, so there is nothing to check.
		if opSource.Address() == issuer {
			return xdr.LedgerEntryChanges{}, nil
		}
		line, ok := lookupTrustline(before, opSource, p.Asset)
		if !ok {
			return nil, wouldFail("source account %s holds no trustline for %s", opSource.Address(), p.Asset.StringCanonical())
		}
		if line.Flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag) == 0 {
			return nil, wouldFail("source trustline for %s is not authorized", p.Asset.StringCanonical())
		}
		if int64(line.Limit)-int64(line.Balance)-int64(line.Liabilities().Buying) < int64(p.Amount) {
			return nil, wouldFail("payment would exceed destination trustline limit for %s", p.Asset.StringCanonical())
		}
		return xdr.LedgerEntryChanges{}, nil
	}
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

// computeAllowTrustChanges translates the legacy allowTrust operation into the
// equivalent trustline flag change: authorize 0 clears both authorization
// flags, 1 grants full authorization, 2 grants maintain-liabilities only.
func computeAllowTrustChanges(at xdr.AllowTrustOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry) (xdr.LedgerEntryChanges, error) {
	authFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag | xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
	set := at.Authorize
	clear := authFlags &^ at.Authorize
	return computeTrustlineFlagChanges(opSource, at.Trustor, at.Asset.ToAsset(opSource), set, clear, before)
}

// computeTrustlineFlagChanges applies an issuer's flag change to the trustor's
// trustline for the asset. Lowering the authorization level requires the
// issuer's AUTH_REVOCABLE flag, matching Core, and a full revocation is only
// previewed when the trustline has no open offers or pool shares whose
// cancellation the preview cannot model.
func computeTrustlineFlagChanges(issuer, trustor xdr.AccountId, asset xdr.Asset, set, clear xdr.Uint32, before map[string]xdr.LedgerEntry) (xdr.LedgerEntryChanges, error) {
	if asset.GetIssuer() != issuer.Address() {
		return nil, wouldFail("only the issuer of %s can change its trustline flags", asset.StringCanonical())
	}
	if trustor.Equals(issuer) {
		return nil, wouldFail("the issuer cannot hold a trustline for its own asset %s", asset.StringCanonical())
	}
	issuerAccount, ok := lookupAccount(before, issuer)
	if !ok {
		return nil, wouldFail("issuer account %s does not exist", issuer.Address())
	}
	line, ok := lookupTrustline(before, trustor, asset)
	if !ok {
		return nil, wouldFail("account %s holds no trustline for %s", trustor.Address(), asset.StringCanonical())
	}

	newFlags := (line.Flags &^ clear) | set
	authBoth := xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag | xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
	if newFlags&authBoth == authBoth {
		// Core rejects a trustline holding both authorization bits at once
		// (SET_TRUST_LINE_FLAGS_INVALID_STATE).
		return nil, wouldFail("a trustline cannot be both fully authorized and maintain-liabilities-only")
	}
	if trustlineAuthLevel(newFlags) < trustlineAuthLevel(line.Flags) &&
		issuerAccount.Flags&xdr.Uint32(xdr.AccountFlagsAuthRevocableFlag) == 0 {
		return nil, wouldFail("issuer %s cannot revoke authorization without the AUTH_REVOCABLE flag", issuer.Address())
	}

	// Revoking below maintain-liabilities also makes Core cancel the trustor's
	// open offers in the asset and redeem its pool shares, side effects whose
	// entries cannot be enumerated through the declarative footprint (and whose
	// removal can itself fail). Zero liabilities and zero pool uses mean
	// neither exists, so the flag-only preview is exact; otherwise fail closed
	// instead of previewing an incomplete result. Downgrading to
	// maintain-liabilities keeps offers and pool shares alive, so it stays
	// previewable.
	if trustlineAuthLevel(line.Flags) > 0 && trustlineAuthLevel(newFlags) == 0 {
		poolUses := xdr.Int32(0)
		if v1, ok := line.Ext.GetV1(); ok {
			if v2, ok := v1.Ext.GetV2(); ok {
				poolUses = v2.LiquidityPoolUseCount
			}
		}
		liabilities := line.Liabilities()
		if liabilities.Buying != 0 || liabilities.Selling != 0 || poolUses != 0 {
			return nil, fmt.Errorf("%w: revoking this trustline's authorization also cancels its open offers or redeems its pool shares, which cannot be previewed", ErrUnsupportedTransaction)
		}
	}

	after := line
	after.Flags = newFlags
	return trustlineChangePair(line, after), nil
}

// trustlineAuthLevel orders the authorization states: fully authorized (2),
// authorized to maintain liabilities only (1), unauthorized (0). Moving down
// this ladder is a revocation.
func trustlineAuthLevel(flags xdr.Uint32) int {
	switch {
	case flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag) != 0:
		return 2
	case flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag) != 0:
		return 1
	default:
		return 0
	}
}

// computeAccountMergeChanges removes the source account entry and moves its
// entire XLM balance to the destination. The network refuses to merge an
// account that still owns anything beyond its balance, so those are would-fails.
func computeAccountMergeChanges(dest, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, ledgerSeq uint32) (xdr.LedgerEntryChanges, error) {
	if dest.Equals(opSource) {
		return nil, wouldFail("account %s cannot merge into itself", opSource.Address())
	}
	source, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	destAccount, ok := lookupAccount(before, dest)
	if !ok {
		return nil, wouldFail("destination account %s does not exist", dest.Address())
	}
	// Core refuses to merge an immutable account (ACCOUNT_MERGE_IMMUTABLE_SET).
	if source.Flags&xdr.Uint32(xdr.AccountFlagsAuthImmutableFlag) != 0 {
		return nil, wouldFail("account %s has AUTH_IMMUTABLE set and cannot be merged", opSource.Address())
	}
	// Signers count toward NumSubEntries but vanish with the account, so they
	// do not block a merge. Anything beyond them (trustlines, offers, data
	// entries) does.
	if int64(source.NumSubEntries) > int64(len(source.Signers)) {
		return nil, wouldFail("account %s still owns %d subentries (trustlines, offers, or data entries) and cannot be merged", opSource.Address(), int64(source.NumSubEntries)-int64(len(source.Signers)))
	}
	if v1, ok := source.Ext.GetV1(); ok {
		if v2, ok := v1.Ext.GetV2(); ok && v2.NumSponsoring > 0 {
			return nil, wouldFail("account %s sponsors %d reserves and cannot be merged", opSource.Address(), v2.NumSponsoring)
		}
	}
	// CAP-21: an account whose sequence number has been bumped into the current
	// ledger's range cannot be merged, so a pre-signed transaction can never be
	// replayed against a later re-creation of the account.
	if int64(source.SeqNum) >= int64(ledgerSeq+1)<<32 {
		return nil, wouldFail("account %s has a sequence number too far ahead to be merged", opSource.Address())
	}
	if int64(source.Balance) > math.MaxInt64-int64(destAccount.Balance)-int64(destAccount.Liabilities().Buying) {
		return nil, wouldFail("destination account %s cannot receive the merged balance without exceeding the maximum balance", dest.Address())
	}

	destAfter := destAccount
	destAfter.Balance += source.Balance
	removedKey := accountLedgerKey(opSource)
	return append(
		accountChangePair(destAccount, destAfter),
		xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: accountLedgerEntry(source)},
		xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &removedKey},
	), nil
}

// computeCreateClaimableBalanceChanges escrows the stated amount of the asset
// from the source into a new claimable-balance entry that only the listed
// claimants can later claim. Relative time predicates are converted to
// absolute at creation, mirroring Core.
//
// Note: the network also records a base-reserve sponsorship for the new entry
// on the creating account; no processor derives state changes from that
// bookkeeping, so it is not modeled here.
func computeCreateClaimableBalanceChanges(cb xdr.CreateClaimableBalanceOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, id xdr.ClaimableBalanceId, now time.Time) (xdr.LedgerEntryChanges, error) {
	source, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}

	// The creator sponsors one base reserve per claimant, paid in XLM whatever
	// the escrowed asset is. The numSponsoring counter is modeled on the working
	// entry because later operations read it: it raises the creator's minimum
	// balance and blocks merging the creator while the entry exists.
	reserve := int64(len(cb.Claimants)) * processors.BaseReserveStroops
	if int64(source.Balance)-processors.MinimumBalance(source)-int64(source.Liabilities().Selling) < reserve {
		return nil, wouldFail("source account %s cannot afford the %d stroop reserve for the claimable balance", opSource.Address(), reserve)
	}
	srcAfter := source
	bumpNumSponsoring(&srcAfter, uint32(len(cb.Claimants)))

	var changes xdr.LedgerEntryChanges
	clawbackEnabled := false
	switch {
	case cb.Asset.Type == xdr.AssetTypeAssetTypeNative:
		available := int64(source.Balance) - processors.MinimumBalance(source) - int64(source.Liabilities().Selling) - reserve
		if available < int64(cb.Amount) {
			return nil, wouldFail("source account %s has insufficient XLM: available %d stroops, escrowing %d", opSource.Address(), available, cb.Amount)
		}
		srcAfter.Balance -= cb.Amount
		changes = accountChangePair(source, srcAfter)
	case opSource.Address() == cb.Asset.GetIssuer():
		// The issuer escrows newly issued units; there is no trustline side.
		clawbackEnabled = source.Flags&xdr.Uint32(xdr.AccountFlagsAuthClawbackEnabledFlag) != 0
		changes = accountChangePair(source, srcAfter)
	default:
		line, ok := lookupTrustline(before, opSource, cb.Asset)
		if !ok {
			return nil, wouldFail("source account %s holds no trustline for %s", opSource.Address(), cb.Asset.StringCanonical())
		}
		if line.Flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag) == 0 {
			return nil, wouldFail("source account %s is not authorized to send %s", opSource.Address(), cb.Asset.StringCanonical())
		}
		available := int64(line.Balance) - int64(line.Liabilities().Selling)
		if available < int64(cb.Amount) {
			return nil, wouldFail("source account %s has insufficient %s: available %d stroops, escrowing %d", opSource.Address(), cb.Asset.StringCanonical(), available, cb.Amount)
		}
		clawbackEnabled = line.Flags&xdr.Uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag) != 0
		after := line
		after.Balance -= cb.Amount
		changes = append(accountChangePair(source, srcAfter), trustlineChangePair(line, after)...)
	}

	claimants := make([]xdr.Claimant, len(cb.Claimants))
	for i, claimant := range cb.Claimants {
		v0 := claimant.MustV0()
		v0.Predicate = absolutePredicate(v0.Predicate, now)
		claimants[i] = xdr.Claimant{Type: xdr.ClaimantTypeClaimantTypeV0, V0: &v0}
	}
	entry := xdr.ClaimableBalanceEntry{
		BalanceId: id,
		Claimants: claimants,
		Asset:     cb.Asset,
		Amount:    cb.Amount,
	}
	if clawbackEnabled {
		entry.Ext = xdr.ClaimableBalanceEntryExt{V: 1, V1: &xdr.ClaimableBalanceEntryExtensionV1{
			Flags: xdr.Uint32(xdr.ClaimableBalanceFlagsClaimableBalanceClawbackEnabledFlag),
		}}
	}
	return append(changes, xdr.LedgerEntryChange{
		Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: claimableBalanceLedgerEntry(entry),
	}), nil
}

// computeClaimClaimableBalanceChanges removes the claimable-balance entry and
// credits its amount to the claiming account, which must be one of the entry's
// claimants with a satisfied predicate.
func computeClaimClaimableBalanceChanges(op xdr.ClaimClaimableBalanceOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry, now time.Time) (xdr.LedgerEntryChanges, error) {
	source, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	cb, ok := lookupClaimableBalance(before, op.BalanceId)
	if !ok {
		return nil, wouldFail("the claimable balance does not exist")
	}

	claimed := false
	for _, claimant := range cb.Claimants {
		v0 := claimant.MustV0()
		if !v0.Destination.Equals(opSource) {
			continue
		}
		if !predicateSatisfied(v0.Predicate, now) {
			return nil, wouldFail("the claim predicate is not currently satisfied for %s", opSource.Address())
		}
		claimed = true
		break
	}
	if !claimed {
		return nil, wouldFail("account %s is not a claimant of this balance", opSource.Address())
	}

	removedKey := claimableBalanceLedgerKey(op.BalanceId)
	changes := xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: claimableBalanceLedgerEntry(cb)},
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &removedKey},
	}

	switch {
	case cb.Asset.Type == xdr.AssetTypeAssetTypeNative:
		if int64(cb.Amount) > math.MaxInt64-int64(source.Balance)-int64(source.Liabilities().Buying) {
			return nil, wouldFail("account %s cannot receive the claimed balance without exceeding the maximum balance", opSource.Address())
		}
		after := source
		after.Balance += cb.Amount
		return append(changes, accountChangePair(source, after)...), nil
	case opSource.Address() == cb.Asset.GetIssuer():
		// The issuer claiming its own asset burns it; no trustline is touched.
		return changes, nil
	default:
		line, ok := lookupTrustline(before, opSource, cb.Asset)
		if !ok {
			return nil, wouldFail("account %s needs a trustline for %s to claim this balance", opSource.Address(), cb.Asset.StringCanonical())
		}
		if line.Flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag) == 0 {
			return nil, wouldFail("account %s is not authorized to hold %s", opSource.Address(), cb.Asset.StringCanonical())
		}
		// Room is computed by subtraction so a huge claim amount cannot wrap
		// the comparison past the limit (balance plus buying liabilities never
		// exceed the limit on a valid ledger entry).
		if int64(cb.Amount) > int64(line.Limit)-int64(line.Balance)-int64(line.Liabilities().Buying) {
			return nil, wouldFail("claiming this balance would exceed the trustline limit for %s", cb.Asset.StringCanonical())
		}
		after := line
		after.Balance += cb.Amount
		return append(changes, trustlineChangePair(line, after)...), nil
	}
}

// computeClawbackClaimableBalanceChanges removes a clawback-enabled
// claimable-balance entry entirely; the escrowed units are burned.
func computeClawbackClaimableBalanceChanges(op xdr.ClawbackClaimableBalanceOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry) (xdr.LedgerEntryChanges, error) {
	if _, ok := lookupAccount(before, opSource); !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	cb, ok := lookupClaimableBalance(before, op.BalanceId)
	if !ok {
		return nil, wouldFail("the claimable balance does not exist")
	}
	if cb.Asset.GetIssuer() != opSource.Address() {
		return nil, wouldFail("only the issuer of %s can claw back this balance", cb.Asset.StringCanonical())
	}
	v1, ok := cb.Ext.GetV1()
	if !ok || v1.Flags&xdr.Uint32(xdr.ClaimableBalanceFlagsClaimableBalanceClawbackEnabledFlag) == 0 {
		return nil, wouldFail("this claimable balance is not clawback enabled")
	}
	removedKey := claimableBalanceLedgerKey(op.BalanceId)
	return xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: claimableBalanceLedgerEntry(cb)},
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &removedKey},
	}, nil
}

// bumpNumSponsoring raises the account's sponsoring counter, materializing the
// v2 extension if the account does not carry one yet (with the mandatory
// per-signer sponsoring slots), into fresh structs so the shared before entry
// stays untouched.
func bumpNumSponsoring(account *xdr.AccountEntry, n uint32) {
	var v1 xdr.AccountEntryExtensionV1
	if account.Ext.V1 != nil {
		v1 = *account.Ext.V1
	}
	var v2 xdr.AccountEntryExtensionV2
	if v1.Ext.V2 != nil {
		v2 = *v1.Ext.V2
	}
	v2.NumSponsoring += xdr.Uint32(n)
	if v2.SignerSponsoringIDs == nil {
		v2.SignerSponsoringIDs = make([]xdr.SponsorshipDescriptor, len(account.Signers))
	}
	v1.Ext = xdr.AccountEntryExtensionV1Ext{V: 2, V2: &v2}
	account.Ext = xdr.AccountEntryExt{V: 1, V1: &v1}
}

// claimableBalanceID derives the deterministic ID the network would assign to
// the claimable balance created by the operation at opIndex: the SHA-256 of
// the (transaction source, sequence number, operation index) preimage.
func claimableBalanceID(envelope xdr.TransactionEnvelope, opIndex int) (xdr.ClaimableBalanceId, error) {
	preimage := xdr.HashIdPreimage{
		Type: xdr.EnvelopeTypeEnvelopeTypeOpId,
		OperationId: &xdr.HashIdPreimageOperationId{
			SourceAccount: envelope.SourceAccount().ToAccountId(),
			SeqNum:        xdr.SequenceNumber(envelope.SeqNum()),
			OpNum:         xdr.Uint32(opIndex),
		},
	}
	payload, err := preimage.MarshalBinary()
	if err != nil {
		return xdr.ClaimableBalanceId{}, fmt.Errorf("marshaling claimable balance preimage: %w", err)
	}
	hash := xdr.Hash(sha256.Sum256(payload))
	return xdr.ClaimableBalanceId{
		Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0,
		V0:   &hash,
	}, nil
}

// claimantTrustlineKeys lists the trustline entries the transaction's claim
// operations would credit, derivable only after the claimable-balance entries
// themselves have been fetched.
func claimantTrustlineKeys(envelope xdr.TransactionEnvelope, working map[string]xdr.LedgerEntry) []xdr.LedgerKey {
	var keys []xdr.LedgerKey
	for _, op := range envelope.Operations() {
		if op.Body.Type != xdr.OperationTypeClaimClaimableBalance {
			continue
		}
		balanceID := op.Body.ClaimClaimableBalanceOp.BalanceId
		var asset xdr.Asset
		if cb, ok := lookupClaimableBalance(working, balanceID); ok {
			asset = cb.Asset
		} else if created, ok := assetOfCreatedBalance(envelope, balanceID); ok {
			// The balance is created by this same transaction, so it is not on
			// the ledger yet; its asset comes from the create operation.
			asset = created
		} else {
			continue
		}
		if asset.Type == xdr.AssetTypeAssetTypeNative {
			continue
		}
		opSource := classicOperationSource(envelope, op)
		if opSource.Address() == asset.GetIssuer() {
			continue
		}
		keys = append(keys, trustlineLedgerKey(opSource, asset))
	}
	return keys
}

// assetOfCreatedBalance finds the asset of a claimable balance that an earlier
// operation of the same transaction creates, by matching the deterministic ID
// each createClaimableBalance operation would be assigned.
func assetOfCreatedBalance(envelope xdr.TransactionEnvelope, id xdr.ClaimableBalanceId) (xdr.Asset, bool) {
	for i, op := range envelope.Operations() {
		if op.Body.Type != xdr.OperationTypeCreateClaimableBalance {
			continue
		}
		created, err := claimableBalanceID(envelope, i)
		if err != nil {
			continue
		}
		if created.Type == id.Type && created.V0 != nil && id.V0 != nil && *created.V0 == *id.V0 {
			return op.Body.CreateClaimableBalanceOp.Asset, true
		}
	}
	return xdr.Asset{}, false
}

// validateClaimPredicate rejects predicate shapes Core refuses at creation:
// AND/OR need exactly two children, NOT needs one, nesting is capped at four
// levels, and time bounds cannot be negative.
func validateClaimPredicate(p xdr.ClaimPredicate, depth int) error {
	if depth > 4 {
		return wouldFail("claim predicates cannot nest more than four levels deep")
	}
	switch p.Type {
	case xdr.ClaimPredicateTypeClaimPredicateUnconditional:
		return nil
	case xdr.ClaimPredicateTypeClaimPredicateAnd, xdr.ClaimPredicateTypeClaimPredicateOr:
		var children []xdr.ClaimPredicate
		if p.Type == xdr.ClaimPredicateTypeClaimPredicateAnd {
			children = *p.AndPredicates
		} else {
			children = *p.OrPredicates
		}
		if len(children) != 2 {
			return wouldFail("AND/OR claim predicates need exactly two children, got %d", len(children))
		}
		for _, child := range children {
			if err := validateClaimPredicate(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	case xdr.ClaimPredicateTypeClaimPredicateNot:
		if p.NotPredicate == nil || *p.NotPredicate == nil {
			return wouldFail("a NOT claim predicate needs a child predicate")
		}
		return validateClaimPredicate(**p.NotPredicate, depth+1)
	case xdr.ClaimPredicateTypeClaimPredicateBeforeAbsoluteTime:
		if *p.AbsBefore < 0 {
			return wouldFail("claim predicate time bounds cannot be negative")
		}
		return nil
	case xdr.ClaimPredicateTypeClaimPredicateBeforeRelativeTime:
		if *p.RelBefore < 0 {
			return wouldFail("claim predicate time bounds cannot be negative")
		}
		return nil
	default:
		return wouldFail("unknown claim predicate type %d", p.Type)
	}
}

// absolutePredicate mirrors Core's creation-time normalization: relative time
// bounds become absolute deadlines anchored at the creating ledger's close
// time, so stored predicates never carry relative clocks.
func absolutePredicate(p xdr.ClaimPredicate, now time.Time) xdr.ClaimPredicate {
	convertAll := func(ps []xdr.ClaimPredicate) *[]xdr.ClaimPredicate {
		out := make([]xdr.ClaimPredicate, len(ps))
		for i, inner := range ps {
			out[i] = absolutePredicate(inner, now)
		}
		return &out
	}
	switch p.Type {
	case xdr.ClaimPredicateTypeClaimPredicateBeforeRelativeTime:
		// Core saturates the conversion at INT64_MAX, so a far-future relative
		// deadline must not wrap into the past.
		abs := xdr.Int64(math.MaxInt64)
		if int64(*p.RelBefore) < math.MaxInt64-now.Unix() {
			abs = xdr.Int64(now.Unix() + int64(*p.RelBefore))
		}
		return xdr.ClaimPredicate{Type: xdr.ClaimPredicateTypeClaimPredicateBeforeAbsoluteTime, AbsBefore: &abs}
	case xdr.ClaimPredicateTypeClaimPredicateAnd:
		return xdr.ClaimPredicate{Type: p.Type, AndPredicates: convertAll(*p.AndPredicates)}
	case xdr.ClaimPredicateTypeClaimPredicateOr:
		return xdr.ClaimPredicate{Type: p.Type, OrPredicates: convertAll(*p.OrPredicates)}
	case xdr.ClaimPredicateTypeClaimPredicateNot:
		inner := absolutePredicate(**p.NotPredicate, now)
		innerPtr := &inner
		return xdr.ClaimPredicate{Type: p.Type, NotPredicate: &innerPtr}
	default:
		return p
	}
}

// predicateSatisfied evaluates a stored claim predicate at the given time. A
// relative predicate can only appear on an entry created before protocol 15
// finalized creation-time conversion; it is treated as unsatisfied rather
// than guessed at, so the preview fails closed.
func predicateSatisfied(p xdr.ClaimPredicate, now time.Time) bool {
	switch p.Type {
	case xdr.ClaimPredicateTypeClaimPredicateUnconditional:
		return true
	case xdr.ClaimPredicateTypeClaimPredicateAnd:
		for _, inner := range *p.AndPredicates {
			if !predicateSatisfied(inner, now) {
				return false
			}
		}
		return true
	case xdr.ClaimPredicateTypeClaimPredicateOr:
		for _, inner := range *p.OrPredicates {
			if predicateSatisfied(inner, now) {
				return true
			}
		}
		return false
	case xdr.ClaimPredicateTypeClaimPredicateNot:
		return !predicateSatisfied(**p.NotPredicate, now)
	case xdr.ClaimPredicateTypeClaimPredicateBeforeAbsoluteTime:
		return now.Unix() < int64(*p.AbsBefore)
	default:
		return false
	}
}

// computeClawbackChanges removes a stated amount of the issuer's asset from a
// holder's trustline. The trustline must have been created clawback-enabled.
func computeClawbackChanges(cb xdr.ClawbackOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry) (xdr.LedgerEntryChanges, error) {
	from := cb.From.ToAccountId()
	if _, ok := lookupAccount(before, opSource); !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	if cb.Asset.GetIssuer() != opSource.Address() {
		return nil, wouldFail("only the issuer of %s can claw it back", cb.Asset.StringCanonical())
	}
	if from.Equals(opSource) {
		return nil, wouldFail("cannot claw back from the issuer itself")
	}
	line, ok := lookupTrustline(before, from, cb.Asset)
	if !ok {
		return nil, wouldFail("account %s holds no trustline for %s", from.Address(), cb.Asset.StringCanonical())
	}
	if line.Flags&xdr.Uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag) == 0 {
		return nil, wouldFail("trustline for %s is not clawback enabled", cb.Asset.StringCanonical())
	}
	available := int64(line.Balance) - int64(line.Liabilities().Selling)
	if available < int64(cb.Amount) {
		return nil, wouldFail("clawback amount %d exceeds the available balance %d", cb.Amount, available)
	}
	after := line
	after.Balance -= cb.Amount
	return trustlineChangePair(line, after), nil
}

// computeBumpSequenceChanges bumps the source's sequence number forward; a
// bumpTo at or below the current sequence is a successful no-op, matching
// Core. Sequence numbers do not surface as wallet-facing state changes, so the
// preview usually carries only the fee row.
func computeBumpSequenceChanges(bs xdr.BumpSequenceOp, opSource xdr.AccountId, before map[string]xdr.LedgerEntry) (xdr.LedgerEntryChanges, error) {
	account, ok := lookupAccount(before, opSource)
	if !ok {
		return nil, wouldFail("source account %s does not exist", opSource.Address())
	}
	if bs.BumpTo <= account.SeqNum {
		return xdr.LedgerEntryChanges{}, nil
	}
	after := account
	after.SeqNum = bs.BumpTo
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

func claimableBalanceLedgerKey(id xdr.ClaimableBalanceId) xdr.LedgerKey {
	return xdr.LedgerKey{Type: xdr.LedgerEntryTypeClaimableBalance, ClaimableBalance: &xdr.LedgerKeyClaimableBalance{
		BalanceId: id,
	}}
}

func claimableBalanceLedgerEntry(entry xdr.ClaimableBalanceEntry) *xdr.LedgerEntry {
	return &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
		Type:             xdr.LedgerEntryTypeClaimableBalance,
		ClaimableBalance: &entry,
	}}
}

func lookupClaimableBalance(before map[string]xdr.LedgerEntry, id xdr.ClaimableBalanceId) (xdr.ClaimableBalanceEntry, bool) {
	entry, ok := lookupEntry(before, claimableBalanceLedgerKey(id))
	if !ok || entry.Data.Type != xdr.LedgerEntryTypeClaimableBalance {
		return xdr.ClaimableBalanceEntry{}, false
	}
	return *entry.Data.ClaimableBalance, true
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

// accountSpendableBalance is the XLM the account can actually spend: its
// balance minus the locked reserves and the amounts committed to open offers.
func accountSpendableBalance(account xdr.AccountEntry) int64 {
	return int64(account.Balance) - processors.MinimumBalance(account) - int64(account.Liabilities().Selling)
}

// affordSubentry verifies the account can afford the base reserve one more
// subentry locks up and bumps its subentry counter.
func affordSubentry(account *xdr.AccountEntry) error {
	if account.NumSubEntries >= maxAccountSubEntries {
		return wouldFail("account %s is at the %d-subentry limit", account.AccountId.Address(), maxAccountSubEntries)
	}
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
