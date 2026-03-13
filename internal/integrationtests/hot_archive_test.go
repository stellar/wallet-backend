// hot_archive_test.go validates that evicted WASM entries can be recovered from the hot archive.
//
// This test uses a separate Stellar Core configured with short persistent entry TTL
// (TESTING_MINIMUM_PERSISTENT_ENTRY_LIFETIME=10) and aggressive eviction scanning
// (OVERRIDE_EVICTION_PARAMS_FOR_TESTING=true, TESTING_STARTING_EVICTION_SCAN_LEVEL=1).
// After eviction, the test reads the hot archive bucket list and verifies the evicted WASM
// hash is present there but absent from the live bucket list.
package integrationtests

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

func TestHotArchive_EvictedWasmRecovery(t *testing.T) {
	if os.Getenv("ENABLE_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration tests: ENABLE_INTEGRATION_TESTS is not 'true'")
	}

	ctx := context.Background()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to get caller information")
	infraDir := filepath.Join(filepath.Dir(filename), "infrastructure")

	// Phase 1: Start isolated infrastructure with short TTL and aggressive eviction
	t.Log("Phase 1: Starting isolated Stellar Core with short TTL")

	testNet, err := network.New(ctx)
	require.NoError(t, err, "creating test network")
	defer func() { _ = testNet.Remove(ctx) }()

	coreDB, err := createShortTTLCoreDB(ctx, testNet)
	require.NoError(t, err, "creating core DB")
	defer func() { _ = coreDB.Terminate(ctx) }()

	coreContainer, err := createShortTTLStellarCore(ctx, testNet, infraDir)
	require.NoError(t, err, "creating stellar core")
	defer func() { _ = coreContainer.Terminate(ctx) }()

	rpcContainer, err := createShortTTLRPC(ctx, testNet, infraDir)
	require.NoError(t, err, "creating RPC")
	defer func() { _ = rpcContainer.Terminate(ctx) }()

	rpcURL, err := rpcContainer.GetConnectionString(ctx)
	require.NoError(t, err, "getting RPC URL")

	// Phase 2: Wait for protocol upgrade, then upload WASM
	t.Log("Phase 2: Uploading WASM contract")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	coreHTTPPort, err := coreContainer.Container.MappedPort(ctx, "11626/tcp")
	require.NoError(t, err, "getting core HTTP port")
	coreHost, err := coreContainer.Host(ctx)
	require.NoError(t, err, "getting core host")
	coreInfoURL := fmt.Sprintf("http://%s:%s/info", coreHost, coreHTTPPort.Port())

	// Wait for protocol upgrade to take effect before uploading
	err = waitForLedger(ctx, t, httpClient, coreInfoURL, 16, 2*time.Minute)
	require.NoError(t, err, "waiting for protocol upgrade")

	masterKP := keypair.Root(infrastructure.NetworkPassphraseStandalone)
	masterAccount := &txnbuild.SimpleAccount{
		AccountID: masterKP.Address(),
		Sequence:  0,
	}

	wasmBytes, err := os.ReadFile(filepath.Join(infraDir, "testdata", "soroban_increment_contract.wasm"))
	require.NoError(t, err, "reading WASM file")

	expectedHash := xdr.Hash(sha256.Sum256(wasmBytes))
	uploadWasm(ctx, t, httpClient, rpcURL, masterKP, masterAccount, wasmBytes)

	// Query the entry's actual TTL
	entryTTL := getWasmEntryTTL(ctx, t, httpClient, rpcURL, expectedHash)
	t.Logf("WASM entry liveUntilLedger: %d", entryTTL)

	// Phase 3: Wait for eviction
	// After TTL expiry, the eviction scan moves the entry to the hot archive.
	// We wait 50 ledgers past TTL for scan completion + checkpoint publication.
	t.Log("Phase 3: Waiting for WASM entry to be evicted")

	targetLedger := entryTTL + 50
	err = waitForLedger(ctx, t, httpClient, coreInfoURL, targetLedger, 5*time.Minute)
	require.NoError(t, err, "waiting for ledger advancement")

	// Phase 4: Connect to history archive and verify eviction
	t.Log("Phase 4: Verifying eviction via history archive")

	archiveURL, err := getArchiveURL(ctx, coreContainer)
	require.NoError(t, err, "getting archive URL")

	archive, err := historyarchive.Connect(
		archiveURL,
		historyarchive.ArchiveOptions{
			NetworkPassphrase:   infrastructure.NetworkPassphraseStandalone,
			CheckpointFrequency: 8,
		},
	)
	require.NoError(t, err, "connecting to archive")

	rootHAS, err := archive.GetRootHAS()
	require.NoError(t, err, "getting root HAS")
	checkpointMgr := archive.GetCheckpointManager()
	latestCheckpoint := checkpointMgr.PrevCheckpoint(rootHAS.CurrentLedger)
	t.Logf("Latest checkpoint: %d (HAS version: %d)", latestCheckpoint, rootHAS.Version)

	// Verify WASM is NOT in live state (evicted)
	liveReader, err := ingest.NewCheckpointChangeReader(ctx, archive, latestCheckpoint)
	require.NoError(t, err, "creating live checkpoint reader")

	var foundInLiveState bool
	for {
		change, readErr := liveReader.Read()
		if readErr != nil {
			break
		}
		if change.Type == xdr.LedgerEntryTypeContractCode && change.Post != nil {
			codeEntry := change.Post.Data.MustContractCode()
			if codeEntry.Hash == expectedHash {
				foundInLiveState = true
			}
		}
	}
	_ = liveReader.Close()
	require.False(t, foundInLiveState, "WASM should NOT be in live state after eviction")

	// Verify WASM IS in hot archive (recovered)
	var foundInHotArchive bool
	var recoveredCodeLen int
	hotArchiveIter := ingest.NewHotArchiveIterator(ctx, archive, latestCheckpoint)

	for entry, iterErr := range hotArchiveIter {
		require.NoError(t, iterErr, "reading hot archive entry")
		if entry.Data.Type == xdr.LedgerEntryTypeContractCode {
			codeEntry := entry.Data.MustContractCode()
			if codeEntry.Hash == expectedHash {
				foundInHotArchive = true
				recoveredCodeLen = len(codeEntry.Code)
			}
		}
	}

	require.True(t, foundInHotArchive, "evicted WASM hash %x should be present in hot archive", expectedHash)
	require.Greater(t, recoveredCodeLen, 0, "recovered WASM should have bytecode")
	t.Logf("Verification complete: WASM evicted from live state, recovered %d bytes from hot archive", recoveredCodeLen)
}

// getWasmEntryTTL queries the RPC for the WASM entry's liveUntilLedger.
func getWasmEntryTTL(
	ctx context.Context,
	t *testing.T,
	httpClient *http.Client,
	rpcURL string,
	wasmHash xdr.Hash,
) uint32 {
	t.Helper()

	ledgerKey := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{
			Hash: wasmHash,
		},
	}
	keyBytes, err := ledgerKey.MarshalBinary()
	require.NoError(t, err, "marshaling ledger key")
	keyB64 := base64.StdEncoding.EncodeToString(keyBytes)

	reqBody := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getLedgerEntries","params":{"keys":["%s"]}}`, keyB64)
	resp, err := httpClient.Post(rpcURL, "application/json", strings.NewReader(reqBody))
	require.NoError(t, err, "calling getLedgerEntries")
	defer resp.Body.Close()

	var rpcResp struct {
		Result struct {
			Entries []struct {
				LiveUntilLedgerSeq uint32 `json:"liveUntilLedgerSeq"`
			} `json:"entries"`
		} `json:"result"`
	}
	err = json.NewDecoder(resp.Body).Decode(&rpcResp)
	require.NoError(t, err, "decoding getLedgerEntries response")
	require.NotEmpty(t, rpcResp.Result.Entries, "WASM entry not found via getLedgerEntries")

	return rpcResp.Result.Entries[0].LiveUntilLedgerSeq
}

// uploadWasm uploads WASM bytecode to the network via RPC.
func uploadWasm(
	ctx context.Context,
	t *testing.T,
	httpClient *http.Client,
	rpcURL string,
	masterKP *keypair.Full,
	masterAccount *txnbuild.SimpleAccount,
	wasmBytes []byte,
) {
	t.Helper()

	uploadOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
			Wasm: &wasmBytes,
		},
		SourceAccount: masterKP.Address(),
	}

	simTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        masterAccount,
		Operations:           []txnbuild.Operation{uploadOp},
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: false,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "building simulation tx")

	simTxXDR, err := simTx.Base64()
	require.NoError(t, err, "encoding simulation tx")

	simResult, err := infrastructure.SimulateTransactionRPC(httpClient, rpcURL, simTxXDR)
	require.NoError(t, err, "simulating transaction")
	require.Empty(t, simResult.Error, "simulation error: %s", simResult.Error)

	uploadOp.Ext = xdr.TransactionExt{
		V:           1,
		SorobanData: &simResult.TransactionData,
	}

	minResourceFee := int64(0)
	fmt.Sscanf(simResult.MinResourceFee, "%d", &minResourceFee)

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        masterAccount,
		Operations:           []txnbuild.Operation{uploadOp},
		BaseFee:              minResourceFee + txnbuild.MinBaseFee,
		IncrementSequenceNum: true,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	require.NoError(t, err, "building real tx")

	tx, err = tx.Sign(infrastructure.NetworkPassphraseStandalone, masterKP)
	require.NoError(t, err, "signing tx")

	txXDR, err := tx.Base64()
	require.NoError(t, err, "encoding signed tx")

	sendResult, err := infrastructure.SubmitTransactionToRPC(httpClient, rpcURL, txXDR)
	require.NoError(t, err, "submitting tx")
	require.NotEqual(t, "ERROR", sendResult.Status, "tx error: %s", sendResult.ErrorResultXDR)

	err = infrastructure.WaitForTransactionConfirmationHTTP(ctx, t, httpClient, rpcURL, sendResult.Hash, 30)
	require.NoError(t, err, "waiting for tx confirmation")
}

// getArchiveURL returns the HTTP URL for the Stellar Core history archive.
func getArchiveURL(ctx context.Context, coreContainer *infrastructure.TestContainer) (string, error) {
	host, err := coreContainer.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("getting core host: %w", err)
	}
	port, err := coreContainer.GetPort(ctx)
	if err != nil {
		return "", fmt.Errorf("getting archive port: %w", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port), nil
}

// waitForLedger polls Core's /info endpoint until the network reaches the target ledger.
func waitForLedger(ctx context.Context, t *testing.T, client *http.Client, coreInfoURL string, targetLedger uint32, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp, err := client.Get(coreInfoURL)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		var info struct {
			Info struct {
				Ledger struct {
					Num uint32 `json:"num"`
				} `json:"ledger"`
			} `json:"info"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&info)
		_ = resp.Body.Close()
		if decodeErr != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		currentLedger := info.Info.Ledger.Num
		if currentLedger >= targetLedger {
			t.Logf("Network reached ledger %d (target: %d)", currentLedger, targetLedger)
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for ledger %d", targetLedger)
}

// createShortTTLCoreDB starts a PostgreSQL container for the short-TTL Stellar Core.
func createShortTTLCoreDB(ctx context.Context, testNet *testcontainers.DockerNetwork) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Name:  "core-postgres-short-ttl",
		Image: "postgres:9.6.17-alpine",
		Env: map[string]string{
			"POSTGRES_PASSWORD": "mysecretpassword",
			"POSTGRES_DB":       "stellar",
		},
		Networks:     []string{testNet.Name},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating core DB container: %w", err)
	}
	log.Ctx(ctx).Info("Created short-TTL Core DB container")
	return container, nil
}

// createShortTTLStellarCore starts a Stellar Core container with short TTL and aggressive eviction.
func createShortTTLStellarCore(ctx context.Context, testNet *testcontainers.DockerNetwork, infraDir string) (*infrastructure.TestContainer, error) {
	req := testcontainers.ContainerRequest{
		Name:  "stellar-core-short-ttl",
		Image: "stellar/stellar-core:24",
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      filepath.Join(infraDir, "config", "standalone-core-short-ttl.cfg"),
				ContainerFilePath: "/stellar-core.cfg",
				FileMode:          0o644,
			},
			{
				HostFilePath:      filepath.Join(infraDir, "config", "core-start.sh"),
				ContainerFilePath: "/start",
				FileMode:          0o755,
			},
		},
		Entrypoint: []string{"/bin/bash"},
		Cmd:        []string{"/start", "standalone"},
		Networks:   []string{testNet.Name},
		ExposedPorts: []string{
			"11625/tcp",
			"11626/tcp",
			"1570/tcp",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("11626/tcp"),
			wait.ForHTTP("/info").
				WithPort("11626/tcp").
				WithPollInterval(2*time.Second),
			wait.ForLog("Ledger close complete: 8"),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating stellar-core container: %w", err)
	}
	log.Ctx(ctx).Info("Created short-TTL Stellar Core container")

	tc := &infrastructure.TestContainer{
		Container:     container,
		MappedPortStr: "1570", // archive port
	}

	// Trigger protocol upgrade
	httpPort, err := container.MappedPort(ctx, "11626/tcp")
	if err != nil {
		return nil, fmt.Errorf("getting HTTP port: %w", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting host: %w", err)
	}
	upgradeURL := fmt.Sprintf("http://%s:%s/upgrades?mode=set&upgradetime=1970-01-01T00:00:00Z&protocolversion=%d",
		host, httpPort.Port(), infrastructure.DefaultProtocolVersionConst)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(upgradeURL)
	if err != nil {
		return nil, fmt.Errorf("triggering protocol upgrade: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("protocol upgrade failed: %d", resp.StatusCode)
	}
	log.Ctx(ctx).Info("Triggered protocol upgrade on short-TTL Core")

	return tc, nil
}

// createShortTTLRPC starts an RPC container connected to the short-TTL Stellar Core.
func createShortTTLRPC(ctx context.Context, testNet *testcontainers.DockerNetwork, infraDir string) (*infrastructure.TestContainer, error) {
	req := testcontainers.ContainerRequest{
		Name:  "stellar-rpc-short-ttl",
		Image: "stellar/stellar-rpc:24.0.0",
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      filepath.Join(infraDir, "config", "captive-core-short-ttl.cfg"),
				ContainerFilePath: "/config/captive-core-short-ttl.cfg",
				FileMode:          0o644,
			},
			{
				HostFilePath:      filepath.Join(infraDir, "config", "stellar_rpc_config_short_ttl.toml"),
				ContainerFilePath: "/config/stellar_rpc_config_short_ttl.toml",
				FileMode:          0o644,
			},
		},
		Cmd:          []string{"--config-path", "/config/stellar_rpc_config_short_ttl.toml"},
		Networks:     []string{testNet.Name},
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("8000/tcp"),
			wait.ForExec([]string{"sh", "-c", `curl -s -X POST http://localhost:8000 -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","method":"getHealth","id":1}' | grep -q '"result"'`}).
				WithPollInterval(2*time.Second).
				WithStartupTimeout(120*time.Second).
				WithExitCodeMatcher(func(exitCode int) bool { return exitCode == 0 }),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating RPC container: %w", err)
	}
	log.Ctx(ctx).Info("Created short-TTL RPC container")

	return &infrastructure.TestContainer{
		Container:     container,
		MappedPortStr: "8000",
	}, nil
}
