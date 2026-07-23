package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
)

func NewLedgerBackend(ctx context.Context, cfg Configs) (ledgerbackend.LedgerBackend, error) {
	switch cfg.LedgerBackendType {
	case LedgerBackendTypeDatastore:
		return newDatastoreLedgerBackend(ctx, cfg.Datastore, cfg.NetworkPassphrase)
	case LedgerBackendTypeRPC:
		return newRPCLedgerBackend(cfg)
	default:
		return nil, fmt.Errorf("unsupported ledger backend type: %s", cfg.LedgerBackendType)
	}
}

// DatastoreConfig holds the flag/env-driven configuration for the datastore ledger backend.
// LedgersPerFile and FilesPerPartition default to 0, meaning the schema is read from the
// datastore's published manifest (.config.json); set them only for a manifest-less store.
type DatastoreConfig struct {
	BucketPath  string // → params["destination_bucket_path"]
	Region      string // → params["region"] (omitted when empty)
	EndpointURL string // → params["endpoint_url"] (omitted when empty; for minio/custom endpoints)

	BufferSize uint32
	NumWorkers uint32
	RetryLimit uint32
	RetryWait  time.Duration

	LedgersPerFile    uint32 // 0 = read from the datastore manifest
	FilesPerPartition uint32 // 0 = read from the datastore manifest
}

// dataStoreConfig builds the SDK datastore config from the flag/env values. Only S3 is
// supported (both the pubnet data lake and the integration-test minio store are S3-compatible).
func (dc DatastoreConfig) dataStoreConfig(networkPassphrase string) datastore.DataStoreConfig {
	params := map[string]string{"destination_bucket_path": dc.BucketPath}
	if dc.Region != "" {
		params["region"] = dc.Region
	}
	if dc.EndpointURL != "" {
		params["endpoint_url"] = dc.EndpointURL
	}
	return datastore.DataStoreConfig{
		Type:              "S3",
		Params:            params,
		Schema:            datastore.DataStoreSchema{LedgersPerFile: dc.LedgersPerFile, FilesPerPartition: dc.FilesPerPartition},
		NetworkPassphrase: networkPassphrase,
	}
}

func newDatastoreLedgerBackend(ctx context.Context, dc DatastoreConfig, networkPassphrase string) (ledgerbackend.LedgerBackend, error) {
	dsCfg := dc.dataStoreConfig(networkPassphrase)

	dataStore, err := datastore.NewDataStore(ctx, dsCfg)
	if err != nil {
		return nil, fmt.Errorf("creating datastore: %w", err)
	}

	schema, err := datastore.LoadSchema(ctx, dataStore, dsCfg)
	if err != nil {
		return nil, fmt.Errorf("loading datastore schema: %w", err)
	}

	ledgerBackend, err := newDatastoreBackend(
		ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: dc.BufferSize,
			NumWorkers: dc.NumWorkers,
			RetryLimit: dc.RetryLimit,
			RetryWait:  dc.RetryWait,
		},
		dataStore,
		schema,
	)
	if err != nil {
		return nil, fmt.Errorf("creating datastore backend: %w", err)
	}

	return ledgerBackend, nil
}

func newRPCLedgerBackend(cfg Configs) (ledgerbackend.LedgerBackend, error) {
	backend := ledgerbackend.NewRPCLedgerBackend(ledgerbackend.RPCLedgerBackendOptions{
		RPCServerURL: cfg.RPCURL,
		BufferSize:   uint32(cfg.GetLedgersLimit),
	})
	log.Infof("Using RPCLedgerBackend for ledger ingestion with buffer size %d", cfg.GetLedgersLimit)
	return backend, nil
}
