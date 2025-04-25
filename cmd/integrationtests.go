package cmd

import (
	"fmt"
	"go/types"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/integrationtests"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/pkg/wbclient"
)

type integrationTestsCmd struct {
	integrationTests *integrationtests.IntegrationTests
}

type integrationTestsCmdConfig struct {
	BaseFee                       int
	DatabaseURL                   string
	LogLevel                      logrus.Level
	NetworkPassphrase             string
	RPCURL                        string
	ClientAuthPrivateKey          string
	ServerBaseURL                 string
	WalletSourceAccountPrivateKey string
}

func (c *integrationTestsCmd) Command() *cobra.Command {
	cfg := integrationTestsCmdConfig{}

	cfgOpts := config.ConfigOptions{
		utils.BaseFeeOption(&cfg.BaseFee),
		utils.ServerBaseURLOption(&cfg.ServerBaseURL),
		utils.DatabaseURLOption(&cfg.DatabaseURL),
		utils.LogLevelOption(&cfg.LogLevel),
		utils.NetworkPassphraseOption(&cfg.NetworkPassphrase),
		utils.RPCURLOption(&cfg.RPCURL),
		{
			Name:           "client-auth-private-key",
			Usage:          "The private key used to authenticate the client when making HTTP requests to the wallet-backend.",
			OptType:        types.String,
			CustomSetValue: utils.SetConfigOptionStellarPrivateKey,
			ConfigKey:      &cfg.ClientAuthPrivateKey,
			Required:       true,
		},
		{
			Name:           "wallet-source-account-private-key",
			Usage:          "The private key of the source account that will be used to send the transactions for the integration tests",
			OptType:        types.String,
			ConfigKey:      &cfg.WalletSourceAccountPrivateKey,
			CustomSetValue: utils.SetConfigOptionStellarPrivateKey,
			Required:       true,
		},
	}

	// Distribution Account Signature Client options
	signatureClientOpts := utils.SignatureClientOptions{}
	cfgOpts = append(cfgOpts, utils.DistributionAccountSignatureProviderOption(&signatureClientOpts)...)

	cmd := &cobra.Command{
		Use:   "integration-tests",
		Short: "Run end-to-end integration tests",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}

			ctx := cmd.Context()

			dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
			if err != nil {
				return fmt.Errorf("opening connection pool: %w", err)
			}

			signatureClientOpts.DBConnectionPool = dbConnectionPool
			signatureClientOpts.NetworkPassphrase = cfg.NetworkPassphrase
			signatureClient, err := utils.SignatureClientResolver(&signatureClientOpts)
			if err != nil {
				return fmt.Errorf("resolving distribution account signature client: %w", err)
			}

			db, err := dbConnectionPool.SqlxDB(ctx)
			if err != nil {
				return fmt.Errorf("getting sqlx db: %w", err)
			}
			metricsService := metrics.NewMetricsService(db)

			httpClient := http.Client{Timeout: time.Duration(30 * time.Second)}
			rpcService, err := services.NewRPCService(cfg.RPCURL, &httpClient, metricsService)
			if err != nil {
				return fmt.Errorf("instantiating rpc service: %w", err)
			}

			walletSigner, err := keypair.ParseFull(cfg.ClientAuthPrivateKey)
			if err != nil {
				return fmt.Errorf("parsing wallet signing key: %w", err)
			}
			wbClient := wbclient.NewClient(cfg.ServerBaseURL, wbclient.RequestSigner{
				Signer: walletSigner,
			})

			sourceAccountKP, err := keypair.ParseFull(cfg.WalletSourceAccountPrivateKey)
			if err != nil {
				return fmt.Errorf("parsing wallet source account private key: %w", err)
			}

			c.integrationTests, err = integrationtests.NewIntegrationTests(ctx, integrationtests.IntegrationTestsOptions{
				BaseFee:                            int64(cfg.BaseFee),
				NetworkPassphrase:                  cfg.NetworkPassphrase,
				RPCService:                         rpcService,
				WBClient:                           wbClient,
				SourceAccountKP:                    sourceAccountKP,
				DBConnectionPool:                   dbConnectionPool,
				DistributionAccountSignatureClient: signatureClient,
			})
			if err != nil {
				return fmt.Errorf("instantiating channel account services: %w", err)
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			err := c.integrationTests.Run(cmd.Context())
			if err != nil {
				return fmt.Errorf("running integration tests: %w", err)
			}
			return nil
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	return cmd
}
