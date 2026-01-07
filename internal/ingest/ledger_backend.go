package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	goloadtest "github.com/stellar/go-stellar-sdk/ingest/loadtest"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/support/log"
)

func NewLedgerBackend(ctx context.Context, cfg Configs) (ledgerbackend.LedgerBackend, error) {
	switch cfg.LedgerBackendType {
	case LedgerBackendTypeDatastore:
		return newDatastoreLedgerBackend(ctx, cfg)
	case LedgerBackendTypeRPC:
		return newRPCLedgerBackend(cfg)
	default:
		return nil, fmt.Errorf("unsupported ledger backend type: %s", cfg.LedgerBackendType)
	}
}

func newDatastoreLedgerBackend(ctx context.Context, cfg Configs) (ledgerbackend.LedgerBackend, error) {
	storageBackendConfig, err := loadDatastoreBackendConfig(cfg.DatastoreConfigPath)
	if err != nil {
		return nil, fmt.Errorf("loading datastore config: %w", err)
	}

	storageBackendConfig.DataStoreConfig.NetworkPassphrase = cfg.NetworkPassphrase

	dataStore, err := datastore.NewDataStore(ctx, storageBackendConfig.DataStoreConfig)
	if err != nil {
		return nil, fmt.Errorf("creating datastore: %w", err)
	}

	schema, err := datastore.LoadSchema(ctx, dataStore, storageBackendConfig.DataStoreConfig)
	if err != nil {
		return nil, fmt.Errorf("loading datastore schema: %w", err)
	}

	ledgerBackend, err := ledgerbackend.NewBufferedStorageBackend(
		storageBackendConfig.BufferedStorageBackendConfig,
		dataStore,
		schema,
	)
	if err != nil {
		return nil, fmt.Errorf("creating buffered storage backend: %w", err)
	}

	log.Info("Using BufferedStorageBackend for ledger ingestion")
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
}

// NewLoadtestLedgerBackend creates a ledger backend that reads synthetic ledgers from a file.
func NewLoadtestLedgerBackend(cfg LoadtestBackendConfig) ledgerbackend.LedgerBackend {
	backend := goloadtest.NewLedgerBackend(goloadtest.LedgerBackendConfig{
		NetworkPassphrase:   cfg.NetworkPassphrase,
		LedgersFilePath:     cfg.LedgersFilePath,
		LedgerCloseDuration: cfg.LedgerCloseDuration,
	})
	log.Infof("Using LoadtestLedgerBackend with file: %s", cfg.LedgersFilePath)
	return backend
}
