package ingest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/services"
)

type Configs struct {
	DatabaseURL          string
	NetworkPassphrase    string
	CaptiveCoreBinPath   string
	CaptiveCoreConfigDir string
	LedgerCursorName     string
	StartLedger          int
	EndLedger            int
	LogLevel             logrus.Level
	AppTracker           apptracker.AppTracker
	RPCURL               string
}

func Ingest(cfg Configs) error {
	ctx := context.Background()

	manager, err := setupDeps(cfg)
	if err != nil {
		log.Ctx(ctx).Fatalf("Error setting up dependencies for ingest: %v", err)
	}

	if err = manager.Run(ctx, uint32(cfg.StartLedger), uint32(cfg.EndLedger)); err != nil {
		log.Ctx(ctx).Fatalf("Running ingest from %d to %d: %v", cfg.StartLedger, cfg.EndLedger, err)
	}

	return nil
}

func setupDeps(cfg Configs) (services.IngestService, error) {
	// Open DB connection pool
	dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to the database: %w", err)
	}
	models, err := data.NewModels(dbConnectionPool)
	if err != nil {
		return nil, fmt.Errorf("creating models: %w", err)
	}

	// Setup Captive Core backend
	captiveCoreConfig, err := getCaptiveCoreConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("getting captive core config: %w", err)
	}
	ledgerBackend, err := ledgerbackend.NewCaptive(captiveCoreConfig)
	if err != nil {
		return nil, fmt.Errorf("creating captive core backend: %w", err)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	rpcService, err := services.NewRPCService(cfg.RPCURL, httpClient)
	if err != nil {
		return nil, fmt.Errorf("instantiating rpc service: %w", err)
	}

	ingestService, err := services.NewIngestService(models, ledgerBackend, cfg.NetworkPassphrase, cfg.LedgerCursorName, cfg.AppTracker, rpcService)
	if err != nil {
		return nil, fmt.Errorf("instantiating ingest service: %w", err)
	}

	return ingestService, nil
}

const (
	configFileNamePubnet  = "stellar-core_pubnet.cfg"
	configFileNameTestnet = "stellar-core_testnet.cfg"
)

func getCaptiveCoreConfig(cfg Configs) (ledgerbackend.CaptiveCoreConfig, error) {
	var networkArchivesURLs []string
	var configFilePath string

	switch cfg.NetworkPassphrase {
	case network.TestNetworkPassphrase:
		networkArchivesURLs = network.TestNetworkhistoryArchiveURLs
		configFilePath = path.Join(cfg.CaptiveCoreConfigDir, configFileNameTestnet)
	case network.PublicNetworkPassphrase:
		networkArchivesURLs = network.PublicNetworkhistoryArchiveURLs
		configFilePath = path.Join(cfg.CaptiveCoreConfigDir, configFileNamePubnet)
	default:
		return ledgerbackend.CaptiveCoreConfig{}, fmt.Errorf("unknown network: %s", cfg.NetworkPassphrase)
	}

	if _, err := os.Stat(configFilePath); errors.Is(err, os.ErrNotExist) {
		return ledgerbackend.CaptiveCoreConfig{}, fmt.Errorf("captive core configuration file not found in %s", configFilePath)
	}

	// Read configuration TOML
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(configFilePath, ledgerbackend.CaptiveCoreTomlParams{
		CoreBinaryPath:     cfg.CaptiveCoreBinPath,
		NetworkPassphrase:  cfg.NetworkPassphrase,
		HistoryArchiveURLs: networkArchivesURLs,
		UseDB:              true,
	})
	if err != nil {
		return ledgerbackend.CaptiveCoreConfig{}, fmt.Errorf("creating captive core toml: %w", err)
	}

	return ledgerbackend.CaptiveCoreConfig{
		NetworkPassphrase:  cfg.NetworkPassphrase,
		HistoryArchiveURLs: networkArchivesURLs,
		BinaryPath:         cfg.CaptiveCoreBinPath,
		Toml:               captiveCoreToml,
		UseDB:              true,
	}, nil
}
