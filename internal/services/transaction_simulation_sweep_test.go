package services

// The bulk equivalence sweep: replays a window of real ledgers and checks the
// classic simulation against what actually happened on chain, at a scale the
// curated fixtures cannot reach. For every qualifying transaction it runs the
// recorded transaction through the real ingestion processors and the bare
// envelope through SimulateStateChanges, then diffs the two.
//
// Pre-state is rebuilt from the window's own metas: each entry's first before
// image seeds a state store, the store advances with every transaction's real
// after images in the network's order, and entries the window never writes are
// fetched from live RPC once. See
// docs/feature-design/equivalence-sweep-data-requirements.md.
//
// It is a discovery tool, not a gate: mismatches are reported, the test does
// not fail on them. Gated because it talks to a live network; never in CI:
//
//	RUN_EQUIVALENCE_SWEEP=true go test ./internal/services/ -run TestBulkEquivalenceSweep -v
//
// Knobs: SWEEP_RPC_URL (default public testnet), SWEEP_NETWORK_PASSPHRASE
// (default testnet), SWEEP_LEDGERS (window size, default 300).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
)

// sweepSupportedOps mirrors the computeClassicChanges dispatcher: a transaction
// qualifies for simulation only if every operation is one of these.
var sweepSupportedOps = map[xdr.OperationType]bool{
	xdr.OperationTypePayment:                  true,
	xdr.OperationTypeCreateAccount:            true,
	xdr.OperationTypeChangeTrust:              true,
	xdr.OperationTypeSetOptions:               true,
	xdr.OperationTypeManageData:               true,
	xdr.OperationTypeSetTrustLineFlags:        true,
	xdr.OperationTypeAllowTrust:               true,
	xdr.OperationTypeClawback:                 true,
	xdr.OperationTypeBumpSequence:             true,
	xdr.OperationTypeAccountMerge:             true,
	xdr.OperationTypeCreateClaimableBalance:   true,
	xdr.OperationTypeClaimClaimableBalance:    true,
	xdr.OperationTypeClawbackClaimableBalance: true,
}

// sweepStore is the tracked ledger state: base64 ledger key to entry, where a
// known-absent entry (created later in the window, or confirmed missing by a
// live fetch) is recorded explicitly, since absence is valid pre-state.
type sweepStore struct {
	entries map[string]*xdr.LedgerEntry // nil value = known absent
}

func sweepKey(change ingest.Change) (string, error) {
	entry := change.Pre
	if entry == nil {
		entry = change.Post
	}
	key, err := entry.LedgerKey()
	if err != nil {
		return "", fmt.Errorf("deriving ledger key: %w", err)
	}
	return xdr.MarshalBase64(key)
}

// seedFirst records the entry's state as of the window start: its first before
// image, or absence when its first appearance is a creation. Later mentions of
// the same key are ignored (first occurrence wins).
func (s *sweepStore) seedFirst(change ingest.Change) error {
	key, err := sweepKey(change)
	if err != nil {
		return err
	}
	if _, seen := s.entries[key]; seen {
		return nil
	}
	if change.Pre != nil {
		pre := *change.Pre
		s.entries[key] = &pre
	} else {
		s.entries[key] = nil
	}
	return nil
}

// shiftFee undoes (or redoes) a fee change's balance delta on the tracked
// entry, leaving every other field as the ledger has advanced it. Restoring
// the fee change's own snapshot instead would wipe whatever earlier
// transactions in the same ledger already did to the account, since fees are
// charged before any operation runs.
func (s *sweepStore) shiftFee(change ingest.Change, undo bool) error {
	if change.Pre == nil || change.Post == nil {
		return nil
	}
	preAcct, preOK := change.Pre.Data.GetAccount()
	postAcct, postOK := change.Post.Data.GetAccount()
	if !preOK || !postOK {
		return nil
	}
	key, err := sweepKey(change)
	if err != nil {
		return err
	}
	entry := s.entries[key]
	if entry == nil || entry.Data.Account == nil {
		return nil
	}
	// Copy before mutating: the stored entry shallow-copies the meta's entry,
	// and the meta still feeds the real side of the comparison.
	acct := *entry.Data.Account
	delta := postAcct.Balance - preAcct.Balance
	if undo {
		acct.Balance -= delta
	} else {
		acct.Balance += delta
	}
	updated := *entry
	updated.Data.Account = &acct
	s.entries[key] = &updated
	return nil
}

// apply advances the store with a transaction's real after image.
func (s *sweepStore) apply(change ingest.Change) error {
	key, err := sweepKey(change)
	if err != nil {
		return err
	}
	if change.Post != nil {
		post := *change.Post
		s.entries[key] = &post
	} else {
		s.entries[key] = nil
	}
	return nil
}

// sweepRPCStub serves GetLedgerEntries from the tracked store, returning only
// the requested keys like real RPC. Keys the window never wrote fall back to
// one live fetch, cached for the rest of the run.
type sweepRPCStub struct {
	RPCServiceMock
	store  *sweepStore
	rpcURL string
	ledger uint32
	fills  int
}

func (s *sweepRPCStub) GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error) {
	entries := make([]entities.LedgerEntryResult, 0, len(keys))
	var missing []xdr.LedgerKey
	for _, key := range keys {
		entry, seen := s.store.entries[key]
		if !seen {
			var ledgerKey xdr.LedgerKey
			if err := xdr.SafeUnmarshalBase64(key, &ledgerKey); err != nil {
				return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("decoding requested key: %w", err)
			}
			missing = append(missing, ledgerKey)
			continue
		}
		if entry == nil {
			continue // known absent
		}
		dataB64, err := xdr.MarshalBase64(entry.Data)
		if err != nil {
			return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("encoding entry data: %w", err)
		}
		entries = append(entries, entities.LedgerEntryResult{
			KeyXDR:             key,
			DataXDR:            dataB64,
			LastModifiedLedger: uint32(entry.LastModifiedLedgerSeq),
		})
	}
	// Never written anywhere in the window means the live value is the window
	// value (small post-window race accepted; findings get rechecked).
	if len(missing) > 0 {
		s.fills++
		fetched, err := sweepLiveFetch(s.rpcURL, missing)
		if err != nil {
			return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("%w: %w", errSweepLiveFill, err)
		}
		found := make(map[string]entities.LedgerEntryResult, len(fetched))
		for _, stored := range fetched {
			found[stored.KeyXDR] = stored
		}
		for _, ledgerKey := range missing {
			keyB64, err := xdr.MarshalBase64(ledgerKey)
			if err != nil {
				return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("encoding key: %w", err)
			}
			stored, ok := found[keyB64]
			if !ok {
				s.store.entries[keyB64] = nil // confirmed absent
				continue
			}
			var data xdr.LedgerEntryData
			if err := xdr.SafeUnmarshalBase64(stored.DataXDR, &data); err != nil {
				return entities.RPCGetLedgerEntriesResult{}, fmt.Errorf("decoding fetched entry: %w", err)
			}
			s.store.entries[keyB64] = &xdr.LedgerEntry{
				LastModifiedLedgerSeq: xdr.Uint32(stored.LastModifiedLedger),
				Data:                  data,
			}
			entries = append(entries, stored)
		}
	}
	return entities.RPCGetLedgerEntriesResult{LatestLedger: s.ledger, Entries: entries}, nil
}

// errSweepLiveFill marks a live-fill failure so the sweep can count the
// transaction as skipped instead of aborting the run: public endpoints
// rate-limit, and one flaky fill must not kill a 60k-transaction sweep.
var errSweepLiveFill = errors.New("live fill failed")

// sweepLiveFetch fetches entries from live RPC with retries, returning errors
// instead of failing the test.
func sweepLiveFetch(rpcURL string, keys []xdr.LedgerKey) ([]entities.LedgerEntryResult, error) {
	encoded := make([]string, 0, len(keys))
	for _, key := range keys {
		b64, err := xdr.MarshalBase64(key)
		if err != nil {
			return nil, fmt.Errorf("encoding ledger key: %w", err)
		}
		encoded = append(encoded, b64)
	}
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "getLedgerEntries",
		"params": map[string]any{"keys": encoded},
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(payload)) //nolint:noctx // gated test helper
		if err != nil {
			lastErr = err
			continue
		}
		var rpcResp struct {
			Result struct {
				Entries []struct {
					KeyXDR             string `json:"key"`
					DataXDR            string `json:"xdr"`
					LastModifiedLedger uint32 `json:"lastModifiedLedgerSeq"`
				} `json:"entries"`
			} `json:"result"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&rpcResp)
		resp.Body.Close() //nolint:errcheck
		if decodeErr != nil {
			lastErr = fmt.Errorf("decoding response (status %d): %w", resp.StatusCode, decodeErr)
			continue
		}
		if rpcResp.Error != nil {
			lastErr = fmt.Errorf("rpc error: %s", rpcResp.Error.Message)
			continue
		}
		out := make([]entities.LedgerEntryResult, 0, len(rpcResp.Result.Entries))
		for _, entry := range rpcResp.Result.Entries {
			out = append(out, entities.LedgerEntryResult{
				KeyXDR:             entry.KeyXDR,
				DataXDR:            entry.DataXDR,
				LastModifiedLedger: entry.LastModifiedLedger,
			})
		}
		return out, nil
	}
	return nil, lastErr
}

func (e storedLedgerEntry) toLedgerEntryResult() entities.LedgerEntryResult {
	return entities.LedgerEntryResult{
		KeyXDR:             e.KeyXDR,
		DataXDR:            e.DataXDR,
		LastModifiedLedger: e.LastModifiedLedger,
	}
}

// sweepSkipReason classifies why a transaction is not simulated. Empty means
// it qualifies.
func sweepSkipReason(tx ingest.LedgerTransaction) string {
	if !tx.Result.Successful() {
		return "failed-tx"
	}
	if isSorobanTransaction(tx.Envelope) {
		return "soroban"
	}
	ops := tx.Envelope.Operations()
	for _, op := range ops {
		if !sweepSupportedOps[op.Body.Type] {
			return "unsupported-op:" + op.Body.Type.String()
		}
	}
	expectedFee := int64(len(ops)) * baseFeeStroops
	if tx.Envelope.Type == xdr.EnvelopeTypeEnvelopeTypeTxFeeBump {
		expectedFee += baseFeeStroops
	}
	if int64(tx.Result.Result.FeeCharged) != expectedFee {
		return "fee-variance"
	}
	return ""
}

// sweepFailureCheckable reports whether a failed transaction's failure is one
// the simulation could have predicted: a classic transaction at the standard
// fee whose operations are all supported and whose result says an operation
// failed. Sequence, signature, and fee-bid failures never qualify; the sweep
// cannot reconstruct those.
func sweepFailureCheckable(tx ingest.LedgerTransaction) bool {
	if tx.Result.Result.Result.Code != xdr.TransactionResultCodeTxFailed {
		return false
	}
	if tx.Envelope.Type == xdr.EnvelopeTypeEnvelopeTypeTxFeeBump {
		return false
	}
	if isSorobanTransaction(tx.Envelope) {
		return false
	}
	ops := tx.Envelope.Operations()
	for _, op := range ops {
		if !sweepSupportedOps[op.Body.Type] {
			return false
		}
	}
	return int64(tx.Result.Result.FeeCharged) == int64(len(ops))*baseFeeStroops
}

// sweepInnerOpCode names an operation result's specific code.
func sweepInnerOpCode(tr xdr.OperationResultTr) string {
	switch tr.Type {
	case xdr.OperationTypePayment:
		return tr.MustPaymentResult().Code.String()
	case xdr.OperationTypeCreateAccount:
		return tr.MustCreateAccountResult().Code.String()
	case xdr.OperationTypeChangeTrust:
		return tr.MustChangeTrustResult().Code.String()
	case xdr.OperationTypeSetOptions:
		return tr.MustSetOptionsResult().Code.String()
	case xdr.OperationTypeManageData:
		return tr.MustManageDataResult().Code.String()
	case xdr.OperationTypeSetTrustLineFlags:
		return tr.MustSetTrustLineFlagsResult().Code.String()
	case xdr.OperationTypeAllowTrust:
		return tr.MustAllowTrustResult().Code.String()
	case xdr.OperationTypeClawback:
		return tr.MustClawbackResult().Code.String()
	case xdr.OperationTypeBumpSequence:
		return tr.MustBumpSeqResult().Code.String()
	case xdr.OperationTypeAccountMerge:
		return tr.MustAccountMergeResult().Code.String()
	case xdr.OperationTypeCreateClaimableBalance:
		return tr.MustCreateClaimableBalanceResult().Code.String()
	case xdr.OperationTypeClaimClaimableBalance:
		return tr.MustClaimClaimableBalanceResult().Code.String()
	case xdr.OperationTypeClawbackClaimableBalance:
		return tr.MustClawbackClaimableBalanceResult().Code.String()
	default:
		return tr.Type.String()
	}
}

// sweepFailedOpCodes summarizes which operations failed and how.
func sweepFailedOpCodes(tx ingest.LedgerTransaction) string {
	results, ok := tx.Result.Result.Result.GetResults()
	if !ok || len(results) == 0 {
		return "no operation results"
	}
	var parts []string
	for i, res := range results {
		if res.Code == xdr.OperationResultCodeOpInner {
			code := sweepInnerOpCode(*res.Tr)
			if strings.HasSuffix(code, "Success") {
				continue
			}
			parts = append(parts, fmt.Sprintf("op %d %s", i+1, code))
		} else {
			parts = append(parts, fmt.Sprintf("op %d %s", i+1, res.Code.String()))
		}
	}
	if len(parts) == 0 {
		return "no failing operation"
	}
	return strings.Join(parts, ", ")
}

type sweepMismatch struct {
	ledger uint32
	hash   string
	kind   string // "diff", "derived-error", or "missed-failure"
	detail string
}

func TestBulkEquivalenceSweep(t *testing.T) {
	if os.Getenv("RUN_EQUIVALENCE_SWEEP") != "true" {
		t.Skip("set RUN_EQUIVALENCE_SWEEP=true to sweep a live RPC's retention window")
	}
	rpcURL := os.Getenv("SWEEP_RPC_URL")
	if rpcURL == "" {
		rpcURL = "https://soroban-testnet.stellar.org"
	}
	passphrase := os.Getenv("SWEEP_NETWORK_PASSPHRASE")
	if passphrase == "" {
		passphrase = network.TestNetworkPassphrase
	}
	windowSize := uint32(300)
	if v := os.Getenv("SWEEP_LEDGERS"); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 32)
		require.NoError(t, err, "SWEEP_LEDGERS must be a number")
		windowSize = uint32(parsed)
	}
	ctx := context.Background()

	// Detect the endpoint's actual retention window instead of assuming one,
	// and stay a few ledgers behind the tip so the window is fully closed.
	var health struct {
		Oldest uint32 `json:"oldestLedger"`
		Latest uint32 `json:"latestLedger"`
	}
	rpcCall(t, rpcURL, "getHealth", map[string]any{}, &health)
	end := health.Latest - 2
	start := end - windowSize + 1
	if v := os.Getenv("SWEEP_START_LEDGER"); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 32)
		require.NoError(t, err, "SWEEP_START_LEDGER must be a number")
		start = uint32(parsed)
		end = start + windowSize - 1
	}
	if start <= health.Oldest {
		start = health.Oldest + 1
	}
	t.Logf("sweeping ledgers %d..%d (%d ledgers) on %s", start, end, end-start+1, rpcURL)

	// Download the window's metas and pre-read every ledger's transactions.
	type sweepLedger struct {
		seq uint32
		txs []ingest.LedgerTransaction
	}
	var ledgers []sweepLedger
	cursor := ""
	for uint32(len(ledgers)) < end-start+1 {
		params := map[string]any{"pagination": map[string]any{"limit": 100}}
		if cursor == "" {
			params["startLedger"] = start
		} else {
			params["pagination"].(map[string]any)["cursor"] = cursor
		}
		var page GetLedgersResponse
		rpcCall(t, rpcURL, "getLedgers", params, &page)
		require.NotEmpty(t, page.Ledgers, "getLedgers returned no ledgers (cursor %q)", cursor)
		for _, info := range page.Ledgers {
			if info.Sequence > end {
				break
			}
			var lcm xdr.LedgerCloseMeta
			require.NoError(t, xdr.SafeUnmarshalBase64(info.LedgerMetadata, &lcm), "decoding meta for ledger %d", info.Sequence)
			reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(passphrase, lcm)
			require.NoError(t, err, "reading ledger %d", info.Sequence)
			var txs []ingest.LedgerTransaction
			for {
				tx, err := reader.Read()
				if err == io.EOF {
					break
				}
				require.NoError(t, err, "reading tx in ledger %d", info.Sequence)
				txs = append(txs, tx)
			}
			ledgers = append(ledgers, sweepLedger{seq: info.Sequence, txs: txs})
		}
		cursor = page.Cursor
		if len(page.Ledgers) > 0 && page.Ledgers[len(page.Ledgers)-1].Sequence > end {
			break
		}
	}
	totalTxs := 0
	for _, l := range ledgers {
		totalTxs += len(l.txs)
	}
	t.Logf("downloaded %d ledgers, %d transactions", len(ledgers), totalTxs)

	// Pass 1: seed the store with each entry's first appearance, walking in
	// the network's application order (fees first, then operations).
	store := &sweepStore{entries: map[string]*xdr.LedgerEntry{}}
	seed := func(changes []ingest.Change) {
		for _, change := range changes {
			require.NoError(t, store.seedFirst(change))
		}
	}
	for _, ledger := range ledgers {
		for _, tx := range ledger.txs {
			seed(tx.GetFeeChanges())
		}
		for _, tx := range ledger.txs {
			changes, err := tx.GetChanges()
			require.NoError(t, err)
			seed(changes)
		}
		for _, tx := range ledger.txs {
			seed(tx.GetPostApplyFeeChanges())
		}
	}
	t.Logf("seeded %d entries from first images", len(store.entries))

	// Pass 2: walk the window; simulate qualifying transactions against the
	// tracked pre-state, diff against the real record, then advance the store
	// from the real record regardless of the outcome.
	realService, err := NewTransactionSimulationService(&RPCServiceMock{}, nil, passphrase)
	require.NoError(t, err)
	stub := &sweepRPCStub{store: store, rpcURL: rpcURL}
	derivedService, err := NewTransactionSimulationService(stub, nil, passphrase)
	require.NoError(t, err)

	apply := func(changes []ingest.Change) {
		for _, change := range changes {
			require.NoError(t, store.apply(change))
		}
	}
	skips := map[string]int{}
	simulatedByOp := map[string]int{}
	mismatchByOp := map[string]int{}
	var mismatches []sweepMismatch
	simulated, matched := 0, 0
	failedChecked, failedAgreed := 0, 0

	for _, ledger := range ledgers {
		stub.ledger = ledger.seq
		for _, tx := range ledger.txs {
			apply(tx.GetFeeChanges())
		}
		for _, tx := range ledger.txs {
			reason := sweepSkipReason(tx)
			if reason == "failed-tx" && sweepFailureCheckable(tx) {
				// The failure direction: a transaction the network rejected
				// for a reason the simulation models should be refused too.
				for _, feeChange := range tx.GetFeeChanges() {
					require.NoError(t, store.shiftFee(feeChange, true))
				}
				failedChecked++
				envB64, err := xdr.MarshalBase64(tx.Envelope)
				require.NoError(t, err)
				_, derivedErr := derivedService.SimulateStateChanges(ctx, envB64)
				switch {
				case errors.Is(derivedErr, errSweepLiveFill):
					skips["live-fill-error"]++
				case errors.Is(derivedErr, ErrUnsupportedTransaction):
					skips["unsupported-variant"]++
				case derivedErr != nil:
					failedAgreed++
				default:
					mismatches = append(mismatches, sweepMismatch{
						ledger: ledger.seq, hash: tx.Result.TransactionHash.HexString(), kind: "missed-failure",
						detail: "network: " + sweepFailedOpCodes(tx) + "; the simulation said the transaction would succeed",
					})
				}
				for _, feeChange := range tx.GetFeeChanges() {
					require.NoError(t, store.shiftFee(feeChange, false))
				}
			} else if reason != "" {
				skips[reason]++
			} else {
				// The ledger's fee phase already deducted this transaction's own
				// fee from the store, but the simulation charges it again itself
				// (its callers hand it pre-fee state). Shift just this
				// transaction's fee back on while simulating, then off again.
				for _, feeChange := range tx.GetFeeChanges() {
					require.NoError(t, store.shiftFee(feeChange, true))
				}
				hash := tx.Result.TransactionHash.HexString()
				opNames := map[string]bool{}
				for _, op := range tx.Envelope.Operations() {
					opNames[op.Body.Type.String()] = true
				}

				// The simulation always synthesizes at transaction index 1, so
				// align the real side's index or every operation ID differs.
				realTx := tx
				realTx.Index = 1
				realChanges, realErr := realService.stateChangesForTransaction(ctx, realTx)

				envB64, err := xdr.MarshalBase64(tx.Envelope)
				require.NoError(t, err)
				derived, derivedErr := derivedService.SimulateStateChanges(ctx, envB64)

				switch {
				case realErr != nil:
					skips["real-side-error"]++
				case errors.Is(derivedErr, errSweepLiveFill):
					// Public-endpoint flakiness, not a simulation verdict.
					skips["live-fill-error"]++
				case errors.Is(derivedErr, ErrUnsupportedTransaction):
					// Honestly refused (an unsupported variant inside a
					// supported op type, like a pool-share trustline): the
					// preview says "no preview", which is not a wrong preview.
					skips["unsupported-variant"]++
				case derivedErr != nil:
					simulated++
					mismatches = append(mismatches, sweepMismatch{
						ledger: ledger.seq, hash: hash, kind: "derived-error",
						detail: derivedErr.Error(),
					})
					for op := range opNames {
						simulatedByOp[op]++
						mismatchByOp[op]++
					}
				default:
					simulated++
					realNorm := normalizeForEquivalence(t, realChanges)
					derivedNorm := normalizeForEquivalence(t, derived.StateChanges)
					equal := len(realNorm) == len(derivedNorm)
					if equal {
						for i := range realNorm {
							if realNorm[i] != derivedNorm[i] {
								equal = false
								break
							}
						}
					}
					for op := range opNames {
						simulatedByOp[op]++
					}
					if equal {
						matched++
					} else {
						mismatches = append(mismatches, sweepMismatch{
							ledger: ledger.seq, hash: hash, kind: "diff",
							detail: fmt.Sprintf("real:\n%v\nderived:\n%v", realNorm, derivedNorm),
						})
						for op := range opNames {
							mismatchByOp[op]++
						}
					}
				}
				// Put the fee back the way the fee phase left it.
				for _, feeChange := range tx.GetFeeChanges() {
					require.NoError(t, store.shiftFee(feeChange, false))
				}
			}
			// Advance with the transaction's real operation changes.
			changes, err := tx.GetChanges()
			require.NoError(t, err)
			apply(changes)
		}
		for _, tx := range ledger.txs {
			apply(tx.GetPostApplyFeeChanges())
		}
	}

	// Report. Mismatches are findings to triage, not test failures.
	t.Logf("==== sweep report ====")
	t.Logf("ledgers %d, transactions %d, simulated %d, matched %d, mismatched %d, live fills %d",
		len(ledgers), totalTxs, simulated, matched, len(mismatches), stub.fills)
	t.Logf("failed transactions checked %d, would-fail agreed %d, missed failures %d",
		failedChecked, failedAgreed, failedChecked-failedAgreed)
	var reasons []string
	for reason := range skips {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		t.Logf("skipped %-40s %d", reason, skips[reason])
	}
	var ops []string
	for op := range simulatedByOp {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	for _, op := range ops {
		t.Logf("op %-35s simulated %-6d mismatched %d", op, simulatedByOp[op], mismatchByOp[op])
	}
	for i, mismatch := range mismatches {
		if i == 20 {
			t.Logf("... and %d more mismatches", len(mismatches)-20)
			break
		}
		t.Logf("MISMATCH [%s] ledger %d tx %s\n%s", mismatch.kind, mismatch.ledger, mismatch.hash, mismatch.detail)
	}
}
