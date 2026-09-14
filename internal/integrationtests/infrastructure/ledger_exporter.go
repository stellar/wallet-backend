package infrastructure

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"
)

// Shared object-store + datastore settings. The schema constants MUST match what
// DatastoreEnv() advertises to the migrate container so the exporter's object keys line
// up with what the migrate container's optimizedStorageBackend reads.
const (
	// SeaweedFS provides the S3-compatible store; the tag is pinned so
	// registry-side changes cannot break CI.
	objectStoreImage        = "chrislusf/seaweedfs:4.46"
	objectStoreNetworkAlias = "object-store"
	// The weed S3 gateway's default port.
	objectStorePort = "8333"
	// SeaweedFS runs without S3 authentication here, but the AWS SDK's default
	// credential chain still needs non-empty credentials to sign requests.
	objectStoreAccessKey = "test"
	objectStoreSecretKey = "test"
	// objectStoreNetworkEndpoint is how a sibling container (e.g. the migrate container)
	// reaches the store over the docker network. The host test process uses the
	// testcontainers connection string.
	objectStoreNetworkEndpoint = "http://" + objectStoreNetworkAlias + ":" + objectStorePort

	datastoreBucket            = "ledgers"
	datastoreRegion            = "us-east-1"
	datastoreLedgersPerFile    = uint32(1)
	datastoreFilesPerPartition = uint32(1)
)

// DatastoreEnv returns the env a wallet-backend container needs to read the object-store-backed
// datastore. AWS_* feed the S3 datastore's default credential chain (the SDK refuses to sign
// without them). DATASTORE_* drive the datastore ledger backend; the schema values MUST match
// the exporter's object keys, hence the shared datastore* constants.
func (s *SharedContainers) DatastoreEnv() map[string]string {
	return map[string]string{
		"AWS_ACCESS_KEY_ID":     objectStoreAccessKey,
		"AWS_SECRET_ACCESS_KEY": objectStoreSecretKey,
		"AWS_REGION":            datastoreRegion,

		"DATASTORE_BUCKET_PATH":         datastoreBucket,
		"DATASTORE_REGION":              datastoreRegion,
		"DATASTORE_ENDPOINT_URL":        objectStoreNetworkEndpoint,
		"DATASTORE_LEDGERS_PER_FILE":    fmt.Sprintf("%d", datastoreLedgersPerFile),
		"DATASTORE_FILES_PER_PARTITION": fmt.Sprintf("%d", datastoreFilesPerPartition),
		// Shallow buffer/worker counts: the test sits at the live tip almost immediately, where a
		// deep prefetch only spams the store with 404s for not-yet-exported ledgers.
		"DATASTORE_BUFFER_SIZE": "10",
		"DATASTORE_NUM_WORKERS": "2",
		"DATASTORE_RETRY_LIMIT": "3",
		"DATASTORE_RETRY_WAIT":  "1s",
	}
}

// StartLedgerExporter continuously exports ledgers from the RPC server into the object-store-backed
// datastore, starting at startLedger and following the live tip. It is a minimal galexie: read
// LedgerCloseMeta, wrap one ledger per batch, zstd+XDR encode, and PutFile under the schema's
// object key — the exact bytes the migration's optimizedStorageBackend expects to decode.
//
// It must keep running for the whole migration: the datastore backend uses an unbounded range
// and retries a missing file forever, so a snapshot that stops short would hang the migration.
// The first ledger is exported synchronously so the datastore is non-empty before the caller
// launches the migration (its LoadSchema probe and first GetFile then succeed). Returns a stop
// func that halts the exporter and waits for its goroutine to exit.
func StartLedgerExporter(t *testing.T, rpcURL, objectStoreEndpoint string, startLedger uint32) func() {
	t.Helper()

	// Object-store creds for the SDK's default credential chain (S3 datastore writes).
	t.Setenv("AWS_ACCESS_KEY_ID", objectStoreAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", objectStoreSecretKey)
	t.Setenv("AWS_REGION", datastoreRegion)

	ctx, cancel := context.WithCancel(context.Background())

	ds, err := datastore.NewDataStore(ctx, datastore.DataStoreConfig{
		Type: "S3",
		Params: map[string]string{
			"destination_bucket_path": datastoreBucket,
			"region":                  datastoreRegion,
			"endpoint_url":            objectStoreEndpoint,
		},
	})
	require.NoError(t, err, "creating exporter datastore")

	backend := ledgerbackend.NewRPCLedgerBackend(ledgerbackend.RPCLedgerBackendOptions{RPCServerURL: rpcURL})
	require.NoError(t, backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(startLedger)), "preparing exporter RPC range")

	schema := datastore.DataStoreSchema{LedgersPerFile: datastoreLedgersPerFile, FilesPerPartition: datastoreFilesPerPartition}

	exportLedger := func(seq uint32) error {
		lcm, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return fmt.Errorf("getting ledger %d: %w", seq, err)
		}
		batch := xdr.LedgerCloseMetaBatch{
			StartSequence:    xdr.Uint32(seq),
			EndSequence:      xdr.Uint32(seq),
			LedgerCloseMetas: []xdr.LedgerCloseMeta{lcm},
		}
		encoder := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, &batch)
		if err := ds.PutFile(ctx, schema.GetObjectKeyFromSequenceNumber(seq), encoder, nil); err != nil {
			return fmt.Errorf("putting ledger %d: %w", seq, err)
		}
		return nil
	}

	// Export the first ledger synchronously so the datastore is non-empty before the migration starts.
	require.NoError(t, exportLedger(startLedger), "exporting first ledger")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := startLedger + 1; ; seq++ {
			if err := exportLedger(seq); err != nil {
				if ctx.Err() == nil {
					log.Ctx(ctx).Errorf("ledger exporter stopped: %v", err)
				}
				return
			}
		}
	}()

	return func() {
		cancel()
		wg.Wait()
		_ = backend.Close() //nolint:errcheck
		_ = ds.Close()      //nolint:errcheck
	}
}
