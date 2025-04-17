package cmd

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
	internalUtils "github.com/stellar/wallet-backend/internal/utils"
)

// ChAccCmdServiceInterface is the interface for the channel account command service. It is used to allow mocking the
// service in tests.
type ChAccCmdServiceInterface interface {
	EnsureChannelAccounts(ctx context.Context, chAccService services.ChannelAccountService, amount int64) error
}

// ChAccCmdService is the implementation of the channel account command service. When not mocked, it acts like a proxy
// to the real service.
type ChAccCmdService struct{}

var _ ChAccCmdServiceInterface = (*ChAccCmdService)(nil)

//nolint:wrapcheck // Skipping wrapcheck because this is just a proxy to the service.
func (s *ChAccCmdService) EnsureChannelAccounts(ctx context.Context, chAccService services.ChannelAccountService, amount int64) error {
	return chAccService.EnsureChannelAccounts(ctx, amount)
}

type channelAccountCmdConfigOptions struct {
	DatabaseURL                   string
	RPCURL                        string
	NetworkPassphrase             string
	BaseFee                       int
	DistributionAccountPrivateKey string
	EncryptionPassphrase          string
	LogLevel                      logrus.Level
}

type channelAccountCmd struct {
	channelAccountService services.ChannelAccountService
}

func (c *channelAccountCmd) Command(cmdService ChAccCmdServiceInterface) *cobra.Command {
	cfg := channelAccountCmdConfigOptions{}
	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&cfg.DatabaseURL),
		utils.RPCURLOption(&cfg.RPCURL),
		utils.LogLevelOption(&cfg.LogLevel),
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

			ctx := cmd.Context()
			distAccSigClientOpts.DBConnectionPool = dbConnectionPool
			distAccSigClientOpts.NetworkPassphrase = cfg.NetworkPassphrase
			distAccSigClient, err := utils.SignatureClientResolver(&distAccSigClientOpts)
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

			channelAccountModel := store.ChannelAccountModel{DB: dbConnectionPool}
			privateKeyEncrypter := signingutils.DefaultPrivateKeyEncrypter{}
			c.channelAccountService, err = services.NewChannelAccountService(ctx, services.ChannelAccountServiceOptions{
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
			defer internalUtils.DeferredClose(cmd.Context(), distAccSigClientOpts.DBConnectionPool, "closing distAccSigClient's db connection pool")
			amount, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid [amount] argument=%s", args[0])
			}

			err = cmdService.EnsureChannelAccounts(cmd.Context(), c.channelAccountService, int64(amount))
			if err != nil {
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
