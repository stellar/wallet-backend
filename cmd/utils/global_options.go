package utils

import (
	"go/types"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/config"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/signing"
)

func IngestServerPortOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "ingest-server-port",
		Usage:       "The port for the ingest server.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 8002,
		Required:    false,
	}
}

func AdminPortOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "admin-port",
		Usage:       "Port for admin server exposing pprof endpoints at /debug/pprof. Leave unset to disable.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 0,
		Required:    false,
	}
}

func DatabaseURLOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "database-url",
		Usage:       "Database connection URL.",
		OptType:     types.String,
		ConfigKey:   configKey,
		FlagDefault: "postgres://postgres@localhost:5432/wallet-backend?sslmode=disable",
		Required:    true,
	}
}

func LogLevelOption(configKey *logrus.Level) *config.ConfigOption {
	return &config.ConfigOption{
		Name:           "log-level",
		Usage:          `The log level used in this project. Options: "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", or "PANIC".`,
		OptType:        types.String,
		FlagDefault:    "TRACE",
		ConfigKey:      configKey,
		CustomSetValue: SetConfigOptionLogLevel,
		Required:       false,
	}
}

func ServerBaseURLOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "server-base-url",
		Usage:       "The server base URL",
		OptType:     types.String,
		ConfigKey:   configKey,
		FlagDefault: "http://localhost:8001",
		Required:    true,
	}
}

func NetworkPassphraseOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "network-passphrase",
		Usage:       "Stellar Network Passphrase to connect.",
		OptType:     types.String,
		ConfigKey:   configKey,
		FlagDefault: network.TestNetworkPassphrase,
		Required:    true,
	}
}

func BaseFeeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "base-fee",
		Usage:       "The maximum base fee (in stroops) the host is willing to pay for submitting a Stellar transaction",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 1_000_000, // 0.1 XLM. Contract invocations require a higher base fees than Stellar classic transactions.
		Required:    true,
	}
}

func RPCURLOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "rpc-url",
		Usage:       "The URL of the RPC Server.",
		OptType:     types.String,
		ConfigKey:   configKey,
		FlagDefault: "http://localhost:8000",
		Required:    true,
	}
}

func ChannelAccountEncryptionPassphraseOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:      "channel-account-encryption-passphrase",
		Usage:     "The encryption passphrase used to encrypt/decrypt the channel accounts private keys. A strong passphrase is recommended.",
		OptType:   types.String,
		ConfigKey: configKey,
		Required:  true,
	}
}

func SentryDSNOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:      "tracker-dsn",
		Usage:     "The Sentry DSN",
		OptType:   types.String,
		ConfigKey: configKey,
		Required:  false,
	}
}

func StellarEnvironmentOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:      "stellar-environment",
		Usage:     "The Stellar Environment",
		OptType:   types.String,
		ConfigKey: configKey,
		Required:  true,
	}
}

func DistributionAccountPublicKeyOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:           "distribution-account-public-key",
		Usage:          "The Distribution Account public key.",
		OptType:        types.String,
		CustomSetValue: SetConfigOptionStellarPublicKey,
		ConfigKey:      configKey,
		Required:       true,
	}
}

func DistributionAccountPrivateKeyOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:           "distribution-account-private-key",
		Usage:          `The Distribution Account private key. It's required if the configured signature client is "ENV"`,
		OptType:        types.String,
		CustomSetValue: SetConfigOptionStellarPrivateKey,
		ConfigKey:      configKey,
		Required:       false,
	}
}

func DistributionAccountSignatureClientProviderOption(configKey *signing.SignatureClientType) *config.ConfigOption {
	return &config.ConfigOption{
		Name:           "distribution-account-signature-provider",
		Usage:          "The Distribution Account Signature Client Provider. Options: ENV, KMS",
		OptType:        types.String,
		CustomSetValue: SetConfigOptionSignatureClientProvider,
		ConfigKey:      configKey,
		FlagDefault:    string(signing.EnvSignatureClientType),
		Required:       true,
	}
}

func StartLedgerOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "start-ledger",
		Usage:       "ledger number from which ingestion should start. When not present, ingestion will resume from last synced ledger.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 0,
		Required:    true,
	}
}

func EndLedgerOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "end-ledger",
		Usage:       "ledger number to end on",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 0,
		Required:    true,
	}
}

func GetLedgersLimitOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "get-ledgers-limit",
		Usage:       `The limit for the number of ledgers to fetch from the RPC in a single "getLedgers" call. In production, don't go above 10 if unless your RPC instance has the MAX_GET_LEDGERS_EXECUTION_DURATION >= 5s.`,
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
	}
}

func AWSOptions(awsRegionConfigKey *string, kmsKeyARN *string, required bool) config.ConfigOptions {
	awsOpts := config.ConfigOptions{
		{
			Name:        "aws-region",
			Usage:       `The AWS region. It's required if the configured signature client is "KMS"`,
			OptType:     types.String,
			ConfigKey:   awsRegionConfigKey,
			FlagDefault: "us-east-2",
			Required:    required,
		},
		{
			Name:      "kms-key-arn",
			Usage:     `The KMS Key ARN. It's required if the configured signature client is "KMS"`,
			OptType:   types.String,
			ConfigKey: kmsKeyARN,
			Required:  required,
		},
	}
	return awsOpts
}

func NetworkOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "network",
		Usage:       "Stellar network to connect to",
		OptType:     types.String,
		ConfigKey:   configKey,
		FlagDefault: "testnet",
		Required:    true,
	}
}

func GraphQLComplexityLimitOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "graphql-complexity-limit",
		Usage:       "The maximum complexity limit for GraphQL queries. Complexity is calculated based on fields and pagination parameters.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 1000,
		Required:    false,
	}
}

func MaxAccountsPerBalancesQueryOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "max-accounts-per-balances-query",
		Usage:       "The maximum number of accounts that can be queried in a single balancesByAccountAddresses GraphQL query.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 20,
		Required:    false,
	}
}

func MaxGraphQLWorkerPoolSizeOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "max-graphql-worker-pool-size",
		Usage:       "Maximum number of concurrent workers for GraphQL parallel operations.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100,
		Required:    false,
	}
}

func GraphQLMaxConcurrencyPerRequestOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "graphql-max-concurrency-per-request",
		Usage:       "Maximum number of concurrent worker goroutines per GraphQL request. Limits how much of the worker pool a single request can consume.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 30,
		Required:    false,
	}
}

func GraphQLRateLimitPerSecondOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "graphql-rate-limit-per-second",
		Usage:       "Maximum GraphQL requests per second per IP address. Set to 0 to disable rate limiting.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 10,
		Required:    false,
	}
}

func GraphQLRateLimitBurstOption(configKey *int) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "graphql-rate-limit-burst",
		Usage:       "Maximum burst of GraphQL requests allowed per IP address. Defaults to the per-second rate if not set.",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 20,
		Required:    false,
	}
}

func DistributionAccountSignatureProviderOption(scOpts *SignatureClientOptions) config.ConfigOptions {
	opts := config.ConfigOptions{}
	opts = append(opts, DistributionAccountPublicKeyOption(&scOpts.DistributionAccountPublicKey))
	opts = append(opts, DistributionAccountSignatureClientProviderOption(&scOpts.Type))
	opts = append(opts, DistributionAccountPrivateKeyOption(&scOpts.DistributionAccountSecretKey))
	opts = append(opts, AWSOptions(&scOpts.AWSRegion, &scOpts.KMSKeyARN, false)...)
	return opts
}

// DBPoolOptions returns config options for tuning the pgxpool connection pool.
// maxConns and minConns accept *int (cobra/viper stores ints); convert to int32 in buildPoolConfig.
func DBPoolOptions(maxConns *int, minConns *int, maxConnLifetime *time.Duration, maxConnIdleTime *time.Duration) config.ConfigOptions {
	return config.ConfigOptions{
		{
			Name:        "db-max-conns",
			Usage:       "Maximum number of connections in the DB pool.",
			OptType:     types.Int,
			ConfigKey:   maxConns,
			FlagDefault: int(db.DefaultMaxConns),
			Required:    false,
		},
		{
			Name:        "db-min-conns",
			Usage:       "Minimum number of idle connections kept in the DB pool.",
			OptType:     types.Int,
			ConfigKey:   minConns,
			FlagDefault: int(db.DefaultMinConns),
			Required:    false,
		},
		{
			Name:           "db-max-conn-lifetime",
			Usage:          "Maximum lifetime of a DB connection (Go duration string, e.g. \"5m\").",
			OptType:        types.String,
			CustomSetValue: SetConfigOptionDuration,
			ConfigKey:      maxConnLifetime,
			FlagDefault:    db.DefaultMaxConnLifetime.String(),
			Required:       false,
		},
		{
			Name:           "db-max-conn-idle-time",
			Usage:          "Maximum idle time for a DB connection (Go duration string, e.g. \"10s\").",
			OptType:        types.String,
			CustomSetValue: SetConfigOptionDuration,
			ConfigKey:      maxConnIdleTime,
			FlagDefault:    db.DefaultMaxConnIdleTime.String(),
			Required:       false,
		},
	}
}
