package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/alitto/pond/v2"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/cmd/utils"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	_ "github.com/stellar/wallet-backend/internal/services/sep41" // registers the SEP-41 current-state repairer via init()
	internalutils "github.com/stellar/wallet-backend/internal/utils"
)

type protocolRepairCmd struct{}

func (c *protocolRepairCmd) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "protocol-repair",
		Short: "Repair a protocol's indexed state from network truth",
		Long:  "Parent command for protocol state repair. Use subcommands to select which state to repair.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmd.Help(); err != nil {
				log.Fatalf("Error calling help command: %s", err.Error())
			}
		},
	}

	cmd.AddCommand(c.currentStateCommand())

	return cmd
}

// repairOpts captures the flags for `protocol-repair current-state`.
type repairOpts struct {
	databaseURL          string
	rpcURL               string
	networkPassphrase    string
	logLevel             logrus.Level
	protocolID           string
	contractAddress      string
	accountAddress       string
	all                  bool
	concurrency          int
	disableHTTPKeepalive bool
}

func (c *protocolRepairCmd) currentStateCommand() *cobra.Command {
	var opts repairOpts

	cfgOpts := config.ConfigOptions{
		utils.DatabaseURLOption(&opts.databaseURL),
		// Repair reads truth by simulating the protocol's view functions, so RPC is required.
		utils.RPCURLOption(&opts.rpcURL),
		utils.NetworkPassphraseOption(&opts.networkPassphrase),
		utils.LogLevelOption(&opts.logLevel),
	}

	cmd := &cobra.Command{
		Use:   "current-state",
		Short: "Repair a protocol's current state from network truth",
		Long: "Re-reads current state from the network via RPC simulation and conditionally rewrites rows that drifted, running concurrently with live ingestion. " +
			"Takes the protocol's current-state advisory lock, so it cannot run while a current-state migration is in flight. " +
			"After repairing a protocol, a failed current-state migration for it must be restarted from scratch, not resumed — a resumed window straddling the repair's ledger stamp re-applies deltas the repaired value already contains.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cfgOpts.RequireE(); err != nil {
				return fmt.Errorf("requiring values of config options: %w", err)
			}
			if err := cfgOpts.SetValues(); err != nil {
				return fmt.Errorf("setting values of config options: %w", err)
			}

			log.DefaultLogger.SetLevel(opts.logLevel)

			return validateRepairOpts(&opts)
		},
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.runRepair(&opts)
		},
	}

	if err := cfgOpts.Init(cmd); err != nil {
		log.Fatalf("Error initializing a config option: %s", err.Error())
	}

	cmd.Flags().StringVar(&opts.protocolID, "protocol", "", "Protocol ID whose current state to repair, e.g. SEP41 (required)")
	cmd.Flags().StringVar(&opts.contractAddress, "contract", "", "Limit the run to one contract (C... strkey)")
	cmd.Flags().StringVar(&opts.accountAddress, "account", "", "Limit the run to one holder (G... or C... strkey)")
	cmd.Flags().BoolVar(&opts.all, "all", false, "Repair the protocol's entire current state")
	cmd.Flags().IntVar(&opts.concurrency, "concurrency", 4, "Repair units verified in parallel (the RPC simulation parallelism)")
	cmd.Flags().BoolVar(&opts.disableHTTPKeepalive, "disable-http-keepalives", false, "Open a fresh RPC connection per request; needed when reaching the RPC through kubectl port-forward, at the cost of one TCP+TLS setup per simulation")

	return cmd
}

// validateRepairOpts rejects invocations with no scope: --all is the explicit
// opt-in to a full sweep, so a bare `protocol-repair current-state` cannot mass-repair by
// accident.
func validateRepairOpts(opts *repairOpts) error {
	if opts.protocolID == "" {
		return fmt.Errorf("--protocol is required")
	}
	if opts.contractAddress != "" && !internalutils.IsContractAddress(opts.contractAddress) {
		return fmt.Errorf("--contract %q is not a valid contract address", opts.contractAddress)
	}
	if opts.accountAddress != "" && !internalutils.IsValidStellarAddress(opts.accountAddress) {
		return fmt.Errorf("--account %q is not a valid account or contract address", opts.accountAddress)
	}

	scoped := opts.contractAddress != "" || opts.accountAddress != ""
	if opts.all && scoped {
		return fmt.Errorf("--all cannot be combined with --contract or --account")
	}
	if !opts.all && !scoped {
		return fmt.Errorf("specify --contract and/or --account, or --all to repair the protocol's entire current state")
	}
	if opts.concurrency < 1 {
		return fmt.Errorf("--concurrency must be at least 1")
	}
	return nil
}

func (c *protocolRepairCmd) runRepair(opts *repairOpts) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Open DB connection
	dbPool, err := db.OpenDBConnectionPool(ctx, opts.databaseURL)
	if err != nil {
		return fmt.Errorf("opening database connection: %w", err)
	}
	defer dbPool.Close()

	// Create models
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(dbPool, m.DB)
	if err != nil {
		return fmt.Errorf("creating models: %w", err)
	}

	// Repair issues one simulation per unit, so connection reuse matters at scale:
	// the keep-alive client keeps a warm connection per worker. Port-forward runs
	// opt out via --disable-http-keepalives (see keepAlivesDisabledHTTPClient).
	httpClient := keepAliveHTTPClient(opts.concurrency)
	if opts.disableHTTPKeepalive {
		httpClient = keepAlivesDisabledHTTPClient()
	}
	rpcService, err := services.NewRPCService(opts.rpcURL, opts.networkPassphrase, httpClient, m.RPC)
	if err != nil {
		return fmt.Errorf("creating RPC service: %w", err)
	}

	// Per-protocol repairers read truth through the contract metadata service's
	// simulation primitive; they pull it from ProtocolDeps.
	metadataPool := pond.NewPool(0)
	defer metadataPool.StopAndWait()
	metadataService, err := services.NewContractMetadataService(rpcService, models.Contract, metadataPool)
	if err != nil {
		return fmt.Errorf("creating contract metadata service: %w", err)
	}

	deps := services.ProtocolDeps{
		NetworkPassphrase:       opts.networkPassphrase,
		Models:                  models,
		RPCService:              rpcService,
		ContractMetadataService: metadataService,
		MetricsService:          m,
	}
	repairers, err := services.BuildCurrentStateRepairers(deps, []string{opts.protocolID})
	if err != nil {
		return fmt.Errorf("building current-state repairers: %w", err)
	}

	repairPool := pond.NewPool(opts.concurrency)
	defer repairPool.StopAndWait()
	m.RegisterPoolMetrics("repair", repairPool)

	service := services.NewProtocolCurrentStateRepairService(dbPool, models.Protocols, repairers, repairPool)
	scope := services.RepairScope{
		ContractAddress: opts.contractAddress,
		AccountAddress:  opts.accountAddress,
	}
	if err := service.Run(ctx, opts.protocolID, scope); err != nil {
		return fmt.Errorf("running current-state repair for %s: %w", opts.protocolID, err)
	}

	return nil
}
