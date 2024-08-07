package utils

import (
	"go/types"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/txnbuild"
)

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
		Usage:       "The base fee (in stroops) for submitting a Stellar transaction",
		OptType:     types.Int,
		ConfigKey:   configKey,
		FlagDefault: 100 * txnbuild.MinBaseFee,
		Required:    true,
	}
}

func HorizonClientURLOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:        "horizon-url",
		Usage:       "The URL of the Stellar Horizon server which this application will communicate with.",
		OptType:     types.String,
		ConfigKey:   configKey,
		FlagDefault: horizonclient.DefaultTestNetClient.HorizonURL,
		Required:    true,
	}
}

func DistributionAccountPrivateKeyOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:           "distribution-account-private-key",
		Usage:          "The Distribution Account private key.",
		OptType:        types.String,
		CustomSetValue: SetConfigOptionStellarPrivateKey,
		ConfigKey:      configKey,
		Required:       true,
	}
}

func ChannelAccountEncryptionPassphraseOption(configKey *string) *config.ConfigOption {
	return &config.ConfigOption{
		Name:      "channel-account-encryption-passphrase",
		Usage:     "The Encryption Passphrase used to encrypt the channel accounts private key.",
		OptType:   types.String,
		ConfigKey: configKey,
		Required:  true,
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
