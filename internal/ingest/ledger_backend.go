package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/pelletier/go-toml"
	rpc "github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	goloadtest "github.com/stellar/go-stellar-sdk/ingest/loadtest"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
)

func NewLedgerBackend(ctx context.Context, cfg Configs) (ledgerbackend.LedgerBackend, error) {
	switch cfg.LedgerBackendType {
	case LedgerBackendTypeDatastore:
		return newDatastoreLedgerBackend(ctx, cfg.DatastoreConfigPath, cfg.NetworkPassphrase)
	case LedgerBackendTypeRPC:
		return newRPCLedgerBackend(cfg)
	default:
		return nil, fmt.Errorf("unsupported ledger backend type: %s", cfg.LedgerBackendType)
	}
}

// newDatastoreResources creates the DataStore client and loads the schema
// from the S3 manifest. Shared by both optimizedStorageBackend (live) and
// backfillFetcher (backfill).
func newDatastoreResources(ctx context.Context, configPath string, networkPassphrase string) (
	datastore.DataStore, datastore.DataStoreSchema, ledgerbackend.BufferedStorageBackendConfig, error,
) {
	storageBackendConfig, err := loadDatastoreBackendConfig(configPath)
	if err != nil {
		return nil, datastore.DataStoreSchema{}, ledgerbackend.BufferedStorageBackendConfig{},
			fmt.Errorf("loading datastore config: %w", err)
	}
	storageBackendConfig.DataStoreConfig.NetworkPassphrase = networkPassphrase

	ds, err := datastore.NewDataStore(ctx, storageBackendConfig.DataStoreConfig)
	if err != nil {
		return nil, datastore.DataStoreSchema{}, ledgerbackend.BufferedStorageBackendConfig{},
			fmt.Errorf("creating datastore: %w", err)
	}

	schema, err := datastore.LoadSchema(ctx, ds, storageBackendConfig.DataStoreConfig)
	if err != nil {
		return nil, datastore.DataStoreSchema{}, ledgerbackend.BufferedStorageBackendConfig{},
			fmt.Errorf("loading datastore schema: %w", err)
	}

	return ds, schema, storageBackendConfig.BufferedStorageBackendConfig, nil
}

func newDatastoreLedgerBackend(ctx context.Context, datastoreConfigPath string, networkPassphrase string) (ledgerbackend.LedgerBackend, error) {
	ds, schema, bufConfig, err := newDatastoreResources(ctx, datastoreConfigPath, networkPassphrase)
	if err != nil {
		return nil, err
	}

	ledgerBackend, err := newOptimizedStorageBackend(bufConfig, ds, schema)
	if err != nil {
		return nil, fmt.Errorf("creating optimized storage backend: %w", err)
	}

	log.Infof("Using optimized storage backend with buffer size %d, %d workers",
		bufConfig.BufferSize, bufConfig.NumWorkers)
	return ledgerBackend, nil
}

func newRPCLedgerBackend(cfg Configs) (ledgerbackend.LedgerBackend, error) {
	client := rpc.NewClient(cfg.RPCURL, nil)
	backend := newOptimizedRPCBackend(client, uint32(cfg.GetLedgersLimit))
	log.Infof("Using optimized RPC ledger backend with buffer size %d", cfg.GetLedgersLimit)
	return backend, nil
}

func loadDatastoreBackendConfig(configPath string) (StorageBackendConfig, error) {
	if configPath == "" {
		return StorageBackendConfig{}, fmt.Errorf("datastore config file path is required for datastore backend type")
	}

	cfg, err := toml.LoadFile(configPath)
	if err != nil {
		return StorageBackendConfig{}, fmt.Errorf("loading datastore config file %s: %w", configPath, err)
	}

	var storageBackendConfig StorageBackendConfig
	if err = cfg.Unmarshal(&storageBackendConfig); err != nil {
		return StorageBackendConfig{}, fmt.Errorf("unmarshalling datastore config: %w", err)
	}

	return storageBackendConfig, nil
}

// LoadtestBackendConfig holds configuration for the loadtest ledger backend.
type LoadtestBackendConfig struct {
	NetworkPassphrase   string
	LedgersFilePath     string
	LedgerCloseDuration time.Duration
	DatastoreConfigPath string
}

// NewLoadtestLedgerBackend creates a ledger backend that reads synthetic ledgers from a file.
func NewLoadtestLedgerBackend(ctx context.Context, cfg LoadtestBackendConfig) (ledgerbackend.LedgerBackend, error) {
	datastoreBackend, err := newDatastoreLedgerBackend(ctx, cfg.DatastoreConfigPath, cfg.NetworkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("creating datastore ledger backend: %w", err)
	}
	config := goloadtest.LedgerBackendConfig{
		NetworkPassphrase:   cfg.NetworkPassphrase,
		LedgersFilePath:     cfg.LedgersFilePath,
		LedgerCloseDuration: cfg.LedgerCloseDuration,
		LedgerBackend:       datastoreBackend,
	}
	backend := goloadtest.NewLedgerBackend(config)
	log.Infof("Using LoadtestLedgerBackend with file: %s", cfg.LedgersFilePath)
	return backend, nil
}
