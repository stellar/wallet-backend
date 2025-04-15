package cmd

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
)

type channelAccountCmdConfigOptions struct {
	DatabaseURL                   string
	NetworkPassphrase             string
	BaseFee                       int
	DistributionAccountPrivateKey string
	EncryptionPassphrase          string
}

type channelAccountCmd struct {
	channelAccountService services.ChannelAccountService
}

func (c *channelAccountCmd) Command() *cobra.Command {
	cfg := channelAccountCmdConfigOptions{}
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&cfg.DatabaseURL),
		utils.NetworkPassphraseOption(&cfg.NetworkPassphrase),
		utils.BaseFeeOption(&cfg.BaseFee),
		utils.ChannelAccountEncryptionPassphraseOption(&cfg.EncryptionPassphrase),
	}

	// Distribution Account Signature Client options
	distAccSigClientOpts := utils.SignatureClientOptions{}
	cfgOpts = append(cfgOpts, utils.DistributionAccountSignatureProviderOption(&distAccSigClientOpts)...)

	cmd := &cobra.Command{
		Use:               "channel-account",
		Short:             "Manage channel accounts",
		PersistentPreRunE: utils.DefaultPersistentPreRunE(cfgOpts),
	}

	ensureCmd := &cobra.Command{
		Use:   "ensure {amount}",
		Short: "Ensures that the number of channel accounts is at least the [amount] provided",
		Args:  cobra.ExactArgs(1),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}

			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}

			dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
			if err != nil {
				return fmt.Errorf("opening connection pool: %w", err)
			}

			distAccSigClientOpts.DBConnectionPool = dbConnectionPool
			distAccSigClientOpts.NetworkPassphrase = cfg.NetworkPassphrase
			distAccSigClient, err := utils.SignatureClientResolver(&distAccSigClientOpts)
			if err != nil {
				return fmt.Errorf("resolving distribution account signature client: %w", err)
			}

			db, err := dbConnectionPool.SqlxDB(context.Background())
			if err != nil {
				return fmt.Errorf("getting sqlx db: %w", err)
			}
			metricsService := metrics.NewMetricsService(db)
			httpClient := http.Client{Timeout: time.Duration(30 * time.Second)}
			rpcService, err := services.NewRPCService("http://localhost:8000", &httpClient, metricsService)
			if err != nil {
				return fmt.Errorf("instantiating rpc service: %w", err)
			}

			channelAccountModel := store.ChannelAccountModel{DB: dbConnectionPool}
			privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
			c.channelAccountService, err = services.NewChannelAccountService(cmd.Context(), services.ChannelAccountServiceOptions{
				DB:                                 dbConnectionPool,
				BaseFee:                            int64(cfg.BaseFee),
				DistributionAccountSignatureClient: distAccSigClient,
				ChannelAccountStore:                &channelAccountModel,
				PrivateKeyEncrypter:                &privateKeyEncrypter,
				EncryptionPassphrase:               cfg.EncryptionPassphrase,
				RPCService:                         rpcService,
			})
			if err != nil {
				return fmt.Errorf("instantiating channel account services: %w", err)
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			defer distAccSigClientOpts.DBConnectionPool.Close()
			amount, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid [amount] argument=%s", args[0])
			}

			if err = c.channelAccountService.EnsureChannelAccounts(cmd.Context(), int64(amount)); err != nil {
				return fmt.Errorf("ensuring the number of channel accounts is created: %w", err)
			}

			return nil
		},
	}

	cmd.AddCommand(ensureCmd)

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	return cmd
}
