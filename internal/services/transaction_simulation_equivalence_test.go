package services

// The classic-simulation equivalence harness: the drift guard for the
// self-derived classic source. Each fixture under testdata/equivalence is a
// real testnet transaction (envelope, result, and transaction meta captured by
// TestRefreshEquivalenceFixtures below). The test runs each fixture two ways:
//
//   - real side: the transaction exactly as the network recorded it, through
//     the same in-memory processor pipeline ingestion uses;
//   - derived side: only the unsigned envelope, through SimulateStateChanges,
//     with the fixture's pre-submission ledger-entry snapshot served via a
//     mocked GetLedgerEntries.
//
// The two sides must produce the same state changes. If a handler drifts from
// what the network actually does (a protocol change, or a bug in the
// derivation), this test fails without needing any network access.
//
// Regenerate the fixtures (rarely needed; talks to a real testnet RPC and
// friendbot):
//
//	RUN_EQUIVALENCE_REFRESH=true go test ./internal/services/ -run TestRefreshEquivalenceFixtures -v

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// equivalenceFixture is the on-disk shape written by TestRefreshEquivalenceFixtures
// and consumed by TestClassicSimulationEquivalence.
type equivalenceFixture struct {
	Name          string `json:"name"`
	EnvelopeXDR   string `json:"envelopeXdr"`
	ResultXDR     string `json:"resultXdr"`
	ResultMetaXDR string `json:"resultMetaXdr"`
	Ledger        int64  `json:"ledger"`
	// LedgerEntries is a getLedgerEntries snapshot of every entry the
	// transaction might touch, taken just before submission. Transaction meta
	// only records entries the transaction changed, not the ones it merely
	// read (a payment's destination account, a trustline's issuer), so the
	// derived side needs this genuine pre-state.
	LedgerEntries []storedLedgerEntry `json:"ledgerEntries"`
}

type storedLedgerEntry struct {
	KeyXDR             string `json:"key"`
	DataXDR            string `json:"xdr"`
	LastModifiedLedger uint32 `json:"lastModifiedLedger"`
}

func TestClassicSimulationEquivalence(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("testdata", "equivalence", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no equivalence fixtures found; run TestRefreshEquivalenceFixtures to generate them")

	for _, path := range paths {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var fixture equivalenceFixture
		require.NoError(t, json.Unmarshal(raw, &fixture))

		t.Run(fixture.Name, func(t *testing.T) {
			var envelope xdr.TransactionEnvelope
			require.NoError(t, xdr.SafeUnmarshalBase64(fixture.EnvelopeXDR, &envelope))
			var result xdr.TransactionResult
			require.NoError(t, xdr.SafeUnmarshalBase64(fixture.ResultXDR, &result))
			var meta xdr.TransactionMeta
			require.NoError(t, xdr.SafeUnmarshalBase64(fixture.ResultMetaXDR, &meta))

			// Real side: the transaction as the network recorded it.
			realService, err := NewTransactionSimulationService(&RPCServiceMock{}, nil, network.TestNetworkPassphrase)
			require.NoError(t, err)
			realTx := ingest.LedgerTransaction{
				Index:      1,
				Envelope:   envelope,
				Ledger:     ledgerCloseMetaAt(uint32(fixture.Ledger)),
				Result:     xdr.TransactionResultPair{Result: result},
				UnsafeMeta: meta,
			}
			realChanges, err := realService.stateChangesForTransaction(context.Background(), realTx)
			require.NoError(t, err)

			// Derived side: only the envelope, with the fixture's pre-submission
			// snapshot served through a stub RPC at the transaction's real
			// ledger, so operation IDs line up. The stub returns only the keys
			// each call asks for, like real RPC: a handler that forgets a
			// footprint key must NOT find the entry anyway, or the drift guard
			// would pass over a broken footprint.
			snapshot := make(map[string]entities.LedgerEntryResult, len(fixture.LedgerEntries))
			for _, stored := range fixture.LedgerEntries {
				snapshot[stored.KeyXDR] = entities.LedgerEntryResult{
					KeyXDR:             stored.KeyXDR,
					DataXDR:            stored.DataXDR,
					LastModifiedLedger: stored.LastModifiedLedger,
				}
			}
			stub := &snapshotRPCStub{snapshot: snapshot, ledger: uint32(fixture.Ledger)}
			derivedService, err := NewTransactionSimulationService(stub, nil, network.TestNetworkPassphrase)
			require.NoError(t, err)
			derived, err := derivedService.SimulateStateChanges(context.Background(), fixture.EnvelopeXDR)
			require.NoError(t, err)

			require.Equal(t,
				normalizeForEquivalence(t, realChanges),
				normalizeForEquivalence(t, derived.StateChanges),
				"derived state changes must match what real ingestion produces")
		})
	}
}

// snapshotRPCStub serves GetLedgerEntries from a fixture's snapshot the way
// real RPC would: only the requested keys, and only those that exist.
type snapshotRPCStub struct {
	RPCServiceMock
	snapshot map[string]entities.LedgerEntryResult
	ledger   uint32
}

func (s *snapshotRPCStub) GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error) {
	entries := make([]entities.LedgerEntryResult, 0, len(keys))
	for _, key := range keys {
		if entry, ok := s.snapshot[key]; ok {
			entries = append(entries, entry)
		}
	}
	return entities.RPCGetLedgerEntriesResult{LatestLedger: s.ledger, Entries: entries}, nil
}

func ledgerCloseMetaAt(seq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(seq)},
			},
		},
	}
}

// normalizeForEquivalence zeroes the fields that legitimately differ between
// the two sides (persist-time IDs and timestamps) and sorts deterministically,
// rendering each change as JSON so mismatches read well in failures.
func normalizeForEquivalence(t *testing.T, changes []types.StateChange) []string {
	t.Helper()
	out := make([]string, 0, len(changes))
	for _, sc := range changes {
		sc.StateChangeID = 0
		sc.IngestedAt = time.Time{}
		sc.LedgerCreatedAt = time.Time{}
		payload, err := json.Marshal(sc)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("op=%d %s", sc.OperationID, payload))
	}
	sort.Strings(out)
	return out
}

// TestRefreshEquivalenceFixtures is the fixture generator: it builds one real
// transaction per supported operation pattern, submits it to testnet, and
// captures the envelope, result, transaction meta, and a pre-submission
// ledger-entry snapshot as JSON under testdata/equivalence. It is gated because
// it talks to a live network; CI only runs the offline harness above.
func TestRefreshEquivalenceFixtures(t *testing.T) {
	if os.Getenv("RUN_EQUIVALENCE_REFRESH") != "true" {
		t.Skip("skipping fixture refresh; set RUN_EQUIVALENCE_REFRESH=true to regenerate against testnet")
	}
	rpcURL := os.Getenv("SPIKE_RPC_URL")
	if rpcURL == "" {
		rpcURL = "https://soroban-testnet.stellar.org"
	}

	// Two funded accounts: A issues and acts, B holds.
	accountA := keypair.MustRandom()
	accountB := keypair.MustRandom()
	friendbotFund(t, accountA.Address())
	friendbotFund(t, accountB.Address())
	t.Logf("A=%s B=%s", accountA.Address(), accountB.Address())

	newAccount := keypair.MustRandom()
	usdc := txnbuild.CreditAsset{Code: "USDC", Issuer: accountA.Address()}
	homeDomain := "example.com"
	signer := keypair.MustRandom().Address()

	// Each scenario is one transaction, submitted in order because later ones
	// depend on earlier state (the trustline must exist before the credit
	// payment, the data entry before its removal, and so on).
	// bumpTo must exceed A's live sequence when the bump_sequence scenario runs;
	// a testnet sequence starts at ledger<<32 and each earlier scenario adds one,
	// so a +10000 headroom is plenty.
	bumpTarget := fetchSequence(t, rpcURL, accountA.Address()) + 10_000

	scenarios := []struct {
		name   string
		source *keypair.Full
		ops    []txnbuild.Operation
	}{
		// A becomes a revocable, clawback-enabled issuer FIRST, so B's trustline
		// below is created clawback-enabled and the flag/clawback scenarios work.
		{"set_options_issuer_flags", accountA, []txnbuild.Operation{
			&txnbuild.SetOptions{SetFlags: []txnbuild.AccountFlag{txnbuild.AuthRevocable, txnbuild.AuthClawbackEnabled}},
		}},
		{"create_account", accountA, []txnbuild.Operation{
			&txnbuild.CreateAccount{Destination: newAccount.Address(), Amount: "100"},
		}},
		{"payment_native", accountA, []txnbuild.Operation{
			&txnbuild.Payment{Destination: accountB.Address(), Amount: "25", Asset: txnbuild.NativeAsset{}},
		}},
		{"change_trust_add", accountB, []txnbuild.Operation{
			&txnbuild.ChangeTrust{Line: usdc.MustToChangeTrustAsset(), Limit: "1000"},
		}},
		{"payment_credit_issue", accountA, []txnbuild.Operation{
			&txnbuild.Payment{Destination: accountB.Address(), Amount: "100", Asset: usdc},
		}},
		{"payment_credit", accountB, []txnbuild.Operation{
			// B pays some USDC back to the issuer (a burn on the real network),
			// covering the credit-payment-to-issuer side.
			&txnbuild.Payment{Destination: accountA.Address(), Amount: "40", Asset: usdc},
		}},
		{"change_trust_update", accountB, []txnbuild.Operation{
			&txnbuild.ChangeTrust{Line: usdc.MustToChangeTrustAsset(), Limit: "5000"},
		}},
		{"clawback", accountA, []txnbuild.Operation{
			&txnbuild.Clawback{From: accountB.Address(), Amount: "5", Asset: usdc},
		}},
		{"set_trust_line_flags_clear", accountA, []txnbuild.Operation{
			&txnbuild.SetTrustLineFlags{
				Trustor:    accountB.Address(),
				Asset:      usdc,
				ClearFlags: []txnbuild.TrustLineFlag{txnbuild.TrustLineAuthorized},
			},
		}},
		{"allow_trust_authorize", accountA, []txnbuild.Operation{
			//nolint:staticcheck // the deprecated operation is exactly what this scenario pins: the simulation must handle legacy allowTrust transactions.
			&txnbuild.AllowTrust{Trustor: accountB.Address(), Type: usdc, Authorize: true},
		}},
		{"bump_sequence", accountA, []txnbuild.Operation{
			&txnbuild.BumpSequence{BumpTo: bumpTarget},
		}},
		{"set_options", accountA, []txnbuild.Operation{
			&txnbuild.SetOptions{
				HomeDomain: &homeDomain,
				Signer:     &txnbuild.Signer{Address: signer, Weight: 5},
			},
		}},
		{"manage_data_add", accountA, []txnbuild.Operation{
			&txnbuild.ManageData{Name: "config", Value: []byte("v1")},
		}},
		{"manage_data_update", accountA, []txnbuild.Operation{
			&txnbuild.ManageData{Name: "config", Value: []byte("v2")},
		}},
		{"manage_data_remove", accountA, []txnbuild.Operation{
			&txnbuild.ManageData{Name: "config", Value: nil},
		}},
		{"multi_op", accountA, []txnbuild.Operation{
			&txnbuild.Payment{Destination: accountB.Address(), Amount: "3", Asset: txnbuild.NativeAsset{}},
			&txnbuild.ManageData{Name: "note", Value: []byte("multi")},
			&txnbuild.Payment{Destination: accountB.Address(), Amount: "4", Asset: txnbuild.NativeAsset{}},
		}},
	}

	outDir := filepath.Join("testdata", "equivalence")
	require.NoError(t, os.MkdirAll(outDir, 0o755))

	// The superset of entries any scenario can touch; absent ones are simply
	// not returned, which is itself meaningful pre-state (e.g. the account
	// createAccount is about to create).
	usdcAsset := xdr.MustNewCreditAsset("USDC", accountA.Address())
	snapshotKeys := []xdr.LedgerKey{
		accountLedgerKey(xdr.MustAddress(accountA.Address())),
		accountLedgerKey(xdr.MustAddress(accountB.Address())),
		accountLedgerKey(xdr.MustAddress(newAccount.Address())),
		trustlineLedgerKey(xdr.MustAddress(accountA.Address()), usdcAsset),
		trustlineLedgerKey(xdr.MustAddress(accountB.Address()), usdcAsset),
		dataLedgerKey(xdr.MustAddress(accountA.Address()), "config"),
		dataLedgerKey(xdr.MustAddress(accountA.Address()), "note"),
	}

	for _, scenario := range scenarios {
		writeScenario(t, rpcURL, outDir, snapshotKeys, scenario.name, scenario.source, scenario.ops...)
	}

	// Scenarios whose operations depend on an earlier on-chain result: a claim
	// or clawback needs the balance ID the create operation reported, so these
	// are built one at a time from the previous fixture's result.
	created := writeScenario(t, rpcURL, outDir, snapshotKeys, "create_claimable_balance", accountB, &txnbuild.CreateClaimableBalance{
		Amount: "7", Asset: usdc,
		Destinations: []txnbuild.Claimant{txnbuild.NewClaimant(accountB.Address(), nil)},
	})
	claimKeys := append(snapshotKeys, claimableBalanceLedgerKey(createdBalanceID(t, created)))
	writeScenario(t, rpcURL, outDir, claimKeys, "claim_claimable_balance", accountB, &txnbuild.ClaimClaimableBalance{
		BalanceID: balanceIDHex(t, createdBalanceID(t, created)),
	})

	// B's trustline is clawback-enabled (created after A set AUTH_CLAWBACK_ENABLED),
	// so this balance is too, and the issuer can claw it back.
	forClawback := writeScenario(t, rpcURL, outDir, snapshotKeys, "create_claimable_balance_clawback", accountB, &txnbuild.CreateClaimableBalance{
		Amount: "5", Asset: usdc,
		Destinations: []txnbuild.Claimant{txnbuild.NewClaimant(accountA.Address(), nil)},
	})
	clawbackKeys := append(snapshotKeys, claimableBalanceLedgerKey(createdBalanceID(t, forClawback)))
	writeScenario(t, rpcURL, outDir, clawbackKeys, "clawback_claimable_balance", accountA, &txnbuild.ClawbackClaimableBalance{
		BalanceID: balanceIDHex(t, createdBalanceID(t, forClawback)),
	})

	// newAccount owns nothing beyond its balance, so it can merge; run this
	// last so no later scenario needs the account.
	writeScenario(t, rpcURL, outDir, snapshotKeys, "account_merge", newAccount, &txnbuild.AccountMerge{
		Destination: accountA.Address(),
	})

	// A payment bidding double the base fee: the network still charges only the
	// base fee, pinning that a padded bid does not inflate the derived fee row.
	writeScenarioWithBaseFee(t, rpcURL, outDir, snapshotKeys, "payment_padded_bid", accountB, 2*txnbuild.MinBaseFee, &txnbuild.Payment{
		Destination: accountA.Address(), Amount: "1", Asset: txnbuild.NativeAsset{},
	})

	// A fee-bumped payment: B's transaction, A pays the fee. The bump bids the
	// minimum base fee.
	writeFeeBumpScenario(t, rpcURL, outDir, snapshotKeys, "fee_bump_payment", accountA, accountB, &txnbuild.Payment{
		Destination: accountA.Address(), Amount: "1", Asset: txnbuild.NativeAsset{},
	})
}

// writeFeeBumpScenario snapshots pre-state, submits one fee-bumped transaction
// (innerSource signs the inner transaction, feeSource signs and pays the bump),
// and writes the fixture carrying the UNSIGNED fee-bump envelope.
func writeFeeBumpScenario(t *testing.T, rpcURL, outDir string, snapshotKeys []xdr.LedgerKey, name string, feeSource, innerSource *keypair.Full, ops ...txnbuild.Operation) {
	t.Helper()
	snapshot := snapshotLedgerEntries(t, rpcURL, snapshotKeys)

	seq := fetchSequence(t, rpcURL, innerSource.Address())
	innerTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: innerSource.Address(), Sequence: seq},
		Operations:           ops,
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		IncrementSequenceNum: true,
	})
	require.NoError(t, err)

	// The unsigned envelope is what the fixture replays through the simulation;
	// signatures play no role in state changes.
	unsignedBump, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
		Inner: innerTx, FeeAccount: feeSource.Address(), BaseFee: txnbuild.MinBaseFee,
	})
	require.NoError(t, err)
	unsignedB64, err := unsignedBump.Base64()
	require.NoError(t, err)

	signedInner, err := innerTx.Sign(network.TestNetworkPassphrase, innerSource)
	require.NoError(t, err)
	bump, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
		Inner: signedInner, FeeAccount: feeSource.Address(), BaseFee: txnbuild.MinBaseFee,
	})
	require.NoError(t, err)
	bump, err = bump.Sign(network.TestNetworkPassphrase, feeSource)
	require.NoError(t, err)
	bumpB64, err := bump.Base64()
	require.NoError(t, err)
	hash, err := bump.HashHex(network.TestNetworkPassphrase)
	require.NoError(t, err)

	var sendResult struct {
		Status         string `json:"status"`
		ErrorResultXdr string `json:"errorResultXdr"`
	}
	rpcCall(t, rpcURL, "sendTransaction", map[string]any{"transaction": bumpB64}, &sendResult)
	require.Equalf(t, "PENDING", sendResult.Status, "%s: submit rejected (errorResultXdr=%s)", name, sendResult.ErrorResultXdr)
	confirmed := waitForTransaction(t, rpcURL, name, hash)

	fixture := equivalenceFixture{
		Name:          name,
		EnvelopeXDR:   unsignedB64,
		ResultXDR:     confirmed.ResultXDR,
		ResultMetaXDR: confirmed.ResultMetaXDR,
		Ledger:        confirmed.Ledger,
		LedgerEntries: snapshot,
	}
	payload, err := json.MarshalIndent(fixture, "", "  ")
	require.NoError(t, err)
	path := filepath.Join(outDir, name+".json")
	require.NoError(t, os.WriteFile(path, append(payload, '\n'), 0o644))
	t.Logf("wrote %s (ledger %d)", path, fixture.Ledger)
}

// writeScenario snapshots pre-state, submits one transaction, and writes the
// fixture; it returns the fixture so a later scenario can read its result.
func writeScenario(t *testing.T, rpcURL, outDir string, snapshotKeys []xdr.LedgerKey, name string, source *keypair.Full, ops ...txnbuild.Operation) equivalenceFixture {
	t.Helper()
	return writeScenarioWithBaseFee(t, rpcURL, outDir, snapshotKeys, name, source, txnbuild.MinBaseFee, ops...)
}

// writeScenarioWithBaseFee is writeScenario with an explicit per-operation fee
// bid, for pinning bid-versus-charge behavior against the real network.
func writeScenarioWithBaseFee(t *testing.T, rpcURL, outDir string, snapshotKeys []xdr.LedgerKey, name string, source *keypair.Full, baseFee int64, ops ...txnbuild.Operation) equivalenceFixture {
	t.Helper()
	snapshot := snapshotLedgerEntries(t, rpcURL, snapshotKeys)
	fixture := captureFixture(t, rpcURL, name, source, baseFee, ops...)
	fixture.LedgerEntries = snapshot
	payload, err := json.MarshalIndent(fixture, "", "  ")
	require.NoError(t, err)
	path := filepath.Join(outDir, name+".json")
	require.NoError(t, os.WriteFile(path, append(payload, '\n'), 0o644))
	t.Logf("wrote %s (ledger %d)", path, fixture.Ledger)
	return fixture
}

// createdBalanceID reads the claimable balance ID out of a create fixture's
// on-chain result.
func createdBalanceID(t *testing.T, fixture equivalenceFixture) xdr.ClaimableBalanceId {
	t.Helper()
	var result xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(fixture.ResultXDR, &result))
	opResults, ok := result.OperationResults()
	require.True(t, ok, "fixture %s has no operation results", fixture.Name)
	cbResult := opResults[0].Tr.MustCreateClaimableBalanceResult()
	return cbResult.MustBalanceId()
}

// balanceIDHex renders a balance ID in the hex form txnbuild operations take.
func balanceIDHex(t *testing.T, id xdr.ClaimableBalanceId) string {
	t.Helper()
	payload, err := id.MarshalBinary()
	require.NoError(t, err)
	return hex.EncodeToString(payload)
}

// captureFixture submits one transaction and returns its on-chain record.
func captureFixture(t *testing.T, rpcURL, name string, source *keypair.Full, baseFee int64, ops ...txnbuild.Operation) equivalenceFixture {
	t.Helper()

	seq := fetchSequence(t, rpcURL, source.Address())
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: source.Address(), Sequence: seq},
		Operations:           ops,
		BaseFee:              baseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		IncrementSequenceNum: true,
	})
	require.NoError(t, err)
	signed, err := tx.Sign(network.TestNetworkPassphrase, source)
	require.NoError(t, err)
	txB64, err := signed.Base64()
	require.NoError(t, err)
	hash, err := signed.HashHex(network.TestNetworkPassphrase)
	require.NoError(t, err)

	var sendResult struct {
		Status         string `json:"status"`
		ErrorResultXdr string `json:"errorResultXdr"`
	}
	rpcCall(t, rpcURL, "sendTransaction", map[string]any{"transaction": txB64}, &sendResult)
	require.Equalf(t, "PENDING", sendResult.Status, "%s: submit rejected (errorResultXdr=%s)", name, sendResult.ErrorResultXdr)

	confirmed := waitForTransaction(t, rpcURL, name, hash)
	// The fixture stores the UNSIGNED envelope: the simulation is called with
	// unsigned transactions, and signatures play no role in state changes.
	unsignedB64, err := tx.Base64()
	require.NoError(t, err)
	return equivalenceFixture{
		Name:          name,
		EnvelopeXDR:   unsignedB64,
		ResultXDR:     confirmed.ResultXDR,
		ResultMetaXDR: confirmed.ResultMetaXDR,
		Ledger:        confirmed.Ledger,
	}
}

// snapshotLedgerEntries fetches the given keys and returns them in the
// fixture's stored form; absent entries are simply not returned.
func snapshotLedgerEntries(t *testing.T, rpcURL string, keys []xdr.LedgerKey) []storedLedgerEntry {
	t.Helper()
	encoded := make([]string, 0, len(keys))
	for _, key := range keys {
		b64, err := xdr.MarshalBase64(key)
		require.NoError(t, err)
		encoded = append(encoded, b64)
	}
	var res struct {
		Entries []struct {
			KeyXDR             string `json:"key"`
			DataXDR            string `json:"xdr"`
			LastModifiedLedger uint32 `json:"lastModifiedLedgerSeq"`
		} `json:"entries"`
	}
	rpcCall(t, rpcURL, "getLedgerEntries", map[string]any{"keys": encoded}, &res)
	out := make([]storedLedgerEntry, 0, len(res.Entries))
	for _, entry := range res.Entries {
		out = append(out, storedLedgerEntry{
			KeyXDR:             entry.KeyXDR,
			DataXDR:            entry.DataXDR,
			LastModifiedLedger: entry.LastModifiedLedger,
		})
	}
	return out
}

// fetchSequence returns the account's current sequence number.
func fetchSequence(t *testing.T, rpcURL, address string) int64 {
	t.Helper()
	entries := snapshotLedgerEntries(t, rpcURL, []xdr.LedgerKey{accountLedgerKey(xdr.MustAddress(address))})
	require.NotEmpty(t, entries, "account %s not found on testnet", address)
	var data xdr.LedgerEntryData
	require.NoError(t, xdr.SafeUnmarshalBase64(entries[0].DataXDR, &data))
	return int64(data.MustAccount().SeqNum)
}

func friendbotFund(t *testing.T, address string) {
	t.Helper()
	resp, err := http.Get("https://friendbot.stellar.org/?addr=" + address)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "friendbot funding %s", address)
}

// rpcCall performs one JSON-RPC request and decodes its result into out.
func rpcCall(t *testing.T, rpcURL, method string, params any, out any) {
	t.Helper()
	reqBody, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	require.NoError(t, err)
	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	var rpcResp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rpcResp))
	if rpcResp.Error != nil {
		t.Fatalf("RPC %s error: %s", method, rpcResp.Error.Message)
	}
	require.NoError(t, json.Unmarshal(rpcResp.Result, out))
}

type confirmedTransaction struct {
	ResultXDR     string
	ResultMetaXDR string
	Ledger        int64
}

func waitForTransaction(t *testing.T, rpcURL, name, hash string) confirmedTransaction {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var txResult struct {
			Status        string `json:"status"`
			ResultXDR     string `json:"resultXdr"`
			ResultMetaXDR string `json:"resultMetaXdr"`
			Ledger        int64  `json:"ledger"`
		}
		rpcCall(t, rpcURL, "getTransaction", map[string]any{"hash": hash}, &txResult)
		switch txResult.Status {
		case "SUCCESS":
			return confirmedTransaction{ResultXDR: txResult.ResultXDR, ResultMetaXDR: txResult.ResultMetaXDR, Ledger: txResult.Ledger}
		case "FAILED":
			t.Fatalf("%s: FAILED on-chain (resultXdr=%s)", name, txResult.ResultXDR)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: not confirmed within 30s (last status %s)", name, txResult.Status)
		}
		time.Sleep(2 * time.Second)
	}
}
