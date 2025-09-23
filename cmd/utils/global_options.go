package utils

import (
	"go/types"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/config"

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

func AdminEndpointOption(configKey *string) *config.ConfigOption {
    return &config.ConfigOption{
        Name:        "admin-endpoint",
        Usage:       "Base path where admin endpoints (pprof) are exposed. Leave empty to disable.",
        OptType:     types.String,
        ConfigKey:   configKey,
        FlagDefault: "",
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

func DistributionAccountSignatureProviderOption(scOpts *SignatureClientOptions) config.ConfigOptions {
	opts := config.ConfigOptions{}
	opts = append(opts, DistributionAccountPublicKeyOption(&scOpts.DistributionAccountPublicKey))
	opts = append(opts, DistributionAccountSignatureClientProviderOption(&scOpts.Type))
	opts = append(opts, DistributionAccountPrivateKeyOption(&scOpts.DistributionAccountSecretKey))
	opts = append(opts, AWSOptions(&scOpts.AWSRegion, &scOpts.KMSKeyARN, false)...)
	return opts
}
