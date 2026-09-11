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
	"github.com/stretchr/testify/mock"
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
			// snapshot served through the mocked RPC at the transaction's real
			// ledger, so operation IDs line up.
			entries := make([]entities.LedgerEntryResult, 0, len(fixture.LedgerEntries))
			for _, stored := range fixture.LedgerEntries {
				entries = append(entries, entities.LedgerEntryResult{
					KeyXDR:             stored.KeyXDR,
					DataXDR:            stored.DataXDR,
					LastModifiedLedger: stored.LastModifiedLedger,
				})
			}
			rpcMock := &RPCServiceMock{}
			rpcMock.On("GetLedgerEntries", mock.Anything).
				Return(entities.RPCGetLedgerEntriesResult{LatestLedger: uint32(fixture.Ledger), Entries: entries}, nil)
			derivedService, err := NewTransactionSimulationService(rpcMock, nil, network.TestNetworkPassphrase)
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
			&txnbuild.SetTrustLineFlags{Trustor: accountB.Address(), Asset: usdc,
				ClearFlags: []txnbuild.TrustLineFlag{txnbuild.TrustLineAuthorized}},
		}},
		{"allow_trust_authorize", accountA, []txnbuild.Operation{
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
		snapshot := snapshotLedgerEntries(t, rpcURL, snapshotKeys)
		fixture := captureFixture(t, rpcURL, scenario.name, scenario.source, scenario.ops...)
		fixture.LedgerEntries = snapshot
		payload, err := json.MarshalIndent(fixture, "", "  ")
		require.NoError(t, err)
		path := filepath.Join(outDir, scenario.name+".json")
		require.NoError(t, os.WriteFile(path, append(payload, '\n'), 0o644))
		t.Logf("wrote %s (ledger %d)", path, fixture.Ledger)
	}
}

// captureFixture submits one transaction and returns its on-chain record.
func captureFixture(t *testing.T, rpcURL, name string, source *keypair.Full, ops ...txnbuild.Operation) equivalenceFixture {
	t.Helper()

	seq := fetchSequence(t, rpcURL, source.Address())
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: source.Address(), Sequence: seq},
		Operations:           ops,
		BaseFee:              txnbuild.MinBaseFee,
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
