package utils

import (
	"go/types"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/config"
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
