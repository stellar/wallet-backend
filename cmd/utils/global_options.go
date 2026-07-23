package utils

import (
	"go/types"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/config"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/ingest"
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
		FlagDefault: 5000,
		Required:    false,
	}
}

// GraphQLIntrospectionEnabledOption controls whether GraphQL schema introspection (__schema,
// __type) is served. Defaults to disabled: introspection makes the full schema (including any
// unreleased or internal-only fields) discoverable to anyone who can reach the endpoint.
func GraphQLIntrospectionEnabledOption(configKey *bool) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "graphql-introspection-enabled",
		Usage:       "Whether to enable GraphQL schema introspection (__schema, __type). Disabled by default in production.",
		OptType:     types.Bool,
		ConfigKey:   configKey,
		FlagDefault: false,
		Required:    false,
	}
}

// DBPoolOptions returns config options for tuning the pgxpool connection pool.
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

// DatastoreOptions returns the config options for the datastore ledger backend. Defaults match
// the Stellar pubnet public data lake, so a flag-less invocation reads pubnet ledgers. The
// schema options (ledgers-per-file, files-per-partition) default to 0, meaning the schema is
// read from the datastore's published manifest; set them only for a manifest-less store such as
// the integration-test minio bucket.
func DatastoreOptions(cfg *ingest.DatastoreConfig) config.ConfigOptions {
	return config.ConfigOptions{
		{
			Name:        "datastore-bucket-path",
			Usage:       "Datastore bucket and path holding exported ledgers (S3 destination_bucket_path). Required.",
			OptType:     types.String,
			ConfigKey:   &cfg.BucketPath,
			FlagDefault: "aws-public-blockchain/v1.1/stellar/ledgers/pubnet",
			Required:    true,
		},
		{
			Name:        "datastore-region",
			Usage:       "AWS region of the datastore bucket. Leave empty to use the AWS default chain.",
			OptType:     types.String,
			ConfigKey:   &cfg.Region,
			FlagDefault: "us-east-2",
			Required:    false,
		},
		{
			Name:        "datastore-endpoint-url",
			Usage:       "Custom S3 endpoint URL (e.g. for minio). Leave empty to use the default AWS endpoint.",
			OptType:     types.String,
			ConfigKey:   &cfg.EndpointURL,
			FlagDefault: "",
			Required:    false,
		},
		{
			Name:        "datastore-buffer-size",
			Usage:       "Number of ledger files to prefetch into the datastore read buffer.",
			OptType:     types.Uint32,
			ConfigKey:   &cfg.BufferSize,
			FlagDefault: uint32(100),
			Required:    false,
		},
		{
			Name:        "datastore-num-workers",
			Usage:       "Number of concurrent workers downloading ledger files from the datastore.",
			OptType:     types.Uint32,
			ConfigKey:   &cfg.NumWorkers,
			FlagDefault: uint32(10),
			Required:    false,
		},
		{
			Name:        "datastore-retry-limit",
			Usage:       "Maximum retries for a failed (transient) ledger-file download.",
			OptType:     types.Uint32,
			ConfigKey:   &cfg.RetryLimit,
			FlagDefault: uint32(3),
			Required:    false,
		},
		{
			Name:           "datastore-retry-wait",
			Usage:          "Wait between datastore download retries (Go duration string, e.g. \"5s\").",
			OptType:        types.String,
			CustomSetValue: SetConfigOptionDuration,
			ConfigKey:      &cfg.RetryWait,
			FlagDefault:    "5s",
			Required:       false,
		},
		{
			Name:        "datastore-ledgers-per-file",
			Usage:       "Ledgers per datastore file. 0 reads the schema from the datastore manifest (.config.json).",
			OptType:     types.Uint32,
			ConfigKey:   &cfg.LedgersPerFile,
			FlagDefault: uint32(0),
			Required:    false,
		},
		{
			Name:        "datastore-files-per-partition",
			Usage:       "Files per datastore partition. 0 reads the schema from the datastore manifest (.config.json).",
			OptType:     types.Uint32,
			ConfigKey:   &cfg.FilesPerPartition,
			FlagDefault: uint32(0),
			Required:    false,
		},
	}
}
