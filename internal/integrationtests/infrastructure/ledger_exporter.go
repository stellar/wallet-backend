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

// Shared minio + datastore settings. The schema constants MUST match what DatastoreEnv()
// advertises to the migrate container so the exporter's object keys line up with what the
// migrate container's optimizedStorageBackend reads.
const (
	minioImage        = "minio/minio:latest"
	minioNetworkAlias = "minio"
	minioPort         = "9000"
	minioRootUser     = "minioadmin"
	minioRootPassword = "minioadmin"
	// minioNetworkEndpoint is how a sibling container (e.g. the migrate container) reaches minio
	// over the docker network. The host test process uses the testcontainers connection string.
	minioNetworkEndpoint = "http://" + minioNetworkAlias + ":" + minioPort

	datastoreBucket            = "ledgers"
	datastoreRegion            = "us-east-1"
	datastoreLedgersPerFile    = uint32(1)
	datastoreFilesPerPartition = uint32(1)
)

// DatastoreEnv returns the env a wallet-backend container needs to read the minio-backed
// datastore. AWS_* feed the S3 datastore's default credential chain (it falls back to anonymous
// access, which a private bucket rejects, if absent). DATASTORE_* drive the datastore ledger
// backend; the schema values MUST match the exporter's object keys, hence the shared datastore*
// constants.
func (s *SharedContainers) DatastoreEnv() map[string]string {
	return map[string]string{
		"AWS_ACCESS_KEY_ID":     minioRootUser,
		"AWS_SECRET_ACCESS_KEY": minioRootPassword,
		"AWS_REGION":            datastoreRegion,

		"DATASTORE_BUCKET_PATH":         datastoreBucket,
		"DATASTORE_REGION":              datastoreRegion,
		"DATASTORE_ENDPOINT_URL":        minioNetworkEndpoint,
		"DATASTORE_LEDGERS_PER_FILE":    fmt.Sprintf("%d", datastoreLedgersPerFile),
		"DATASTORE_FILES_PER_PARTITION": fmt.Sprintf("%d", datastoreFilesPerPartition),
		// Shallow buffer/worker counts: the test sits at the live tip almost immediately, where a
		// deep prefetch only spams minio with 404s for not-yet-exported ledgers.
		"DATASTORE_BUFFER_SIZE": "10",
		"DATASTORE_NUM_WORKERS": "2",
		"DATASTORE_RETRY_LIMIT": "3",
		"DATASTORE_RETRY_WAIT":  "1s",
	}
}

// StartLedgerExporter continuously exports ledgers from the RPC server into the minio-backed
// datastore, starting at startLedger and following the live tip. It is a minimal galexie: read
// LedgerCloseMeta, wrap one ledger per batch, zstd+XDR encode, and PutFile under the schema's
// object key — the exact bytes the migration's optimizedStorageBackend expects to decode.
//
// It must keep running for the whole migration: the datastore backend uses an unbounded range
// and retries a missing file forever, so a snapshot that stops short would hang the migration.
// The first ledger is exported synchronously so the datastore is non-empty before the caller
// launches the migration (its LoadSchema probe and first GetFile then succeed). Returns a stop
// func that halts the exporter and waits for its goroutine to exit.
func StartLedgerExporter(t *testing.T, rpcURL, minioEndpoint string, startLedger uint32) func() {
	t.Helper()

	// minio root creds for the SDK's default credential chain (S3 datastore writes).
	t.Setenv("AWS_ACCESS_KEY_ID", minioRootUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioRootPassword)
	t.Setenv("AWS_REGION", datastoreRegion)

	ctx, cancel := context.WithCancel(context.Background())

	ds, err := datastore.NewDataStore(ctx, datastore.DataStoreConfig{
		Type: "S3",
		Params: map[string]string{
			"destination_bucket_path": datastoreBucket,
			"region":                  datastoreRegion,
			"endpoint_url":            minioEndpoint,
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
