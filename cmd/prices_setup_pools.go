package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/services/prices"
)

// pricesSetupPoolsProgressEvery is how many pairs are read between progress log lines.
const pricesSetupPoolsProgressEvery = 50

type pricesSetupPoolsCmd struct{}

func (c *pricesSetupPoolsCmd) Command() *cobra.Command {
	var databaseURL string
	var rpcURL string
	var networkPassphrase string
	var logLevel logrus.Level

	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&databaseURL),
		utils.RPCURLOption(&rpcURL),
		utils.NetworkPassphraseOption(&networkPassphrase),
		utils.LogLevelOption(&logLevel),
	}

	cmd := &cobra.Command{
		Use:   "prices-setup-pools",
		Short: "Seed the AMM pool registry for token prices from the Soroswap factory",
		Long:  "Reads every pair from the Soroswap factory over RPC and registers it in amm_pools, so the trades processor trusts pools created before token prices went live.",
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}
			log.DefaultLogger.SetLevel(logLevel)
			return nil
		},
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.Run(databaseURL, rpcURL, networkPassphrase)
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}
	return cmd
}

func (c *pricesSetupPoolsCmd) Run(databaseURL, rpcURL, networkPassphrase string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	venues, err := processors.TradeVenuesFor(networkPassphrase)
	if err != nil {
		return fmt.Errorf("resolving trade venues: %w", err)
	}
	if venues.SoroswapFactory == "" {
		return fmt.Errorf("no Soroswap factory is configured for network passphrase %q", networkPassphrase)
	}

	dbPool, err := db.OpenDBConnectionPool(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("opening database connection: %w", err)
	}
	defer dbPool.Close()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(dbPool, m.DB)
	if err != nil {
		return fmt.Errorf("creating models: %w", err)
	}

	httpClient := keepAlivesDisabledHTTPClient()
	rpcService, err := services.NewRPCService(rpcURL, networkPassphrase, httpClient, m.RPC)
	if err != nil {
		return fmt.Errorf("creating RPC service: %w", err)
	}
	metadataService, err := services.NewContractMetadataService(rpcService)
	if err != nil {
		return fmt.Errorf("creating contract metadata service: %w", err)
	}

	seeder := &prices.PoolSeeder{Fetcher: metadataService}
	pools, err := seeder.SoroswapPools(ctx, venues.SoroswapFactory, func(done, total int) {
		if done%pricesSetupPoolsProgressEvery == 0 || done == total {
			log.Ctx(ctx).Infof("read %d/%d Soroswap pairs", done, total)
		}
	})
	if err != nil {
		return fmt.Errorf("reading Soroswap pools: %w", err)
	}

	existing, err := models.AMMPools.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("reading registered pools: %w", err)
	}
	known := make(map[string]struct{}, len(existing))
	for _, p := range existing {
		known[p.Pool] = struct{}{}
	}
	newCount := 0
	for _, p := range pools {
		if _, ok := known[p.Pool]; !ok {
			newCount++
		}
	}

	if err := db.RunInTransaction(ctx, dbPool, func(tx pgx.Tx) error {
		return models.AMMPools.BatchUpsert(ctx, tx, pools)
	}); err != nil {
		return fmt.Errorf("registering pools: %w", err)
	}

	log.Ctx(ctx).Infof("registered %d pools (%d new)", len(pools), newCount)
	return nil
}
