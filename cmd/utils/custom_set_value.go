package utils

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/support/config"
	"github.com/stellar/go-stellar-sdk/support/log"
)

func unexpectedTypeError(key any, co *config.ConfigOption) error {
	return fmt.Errorf("the expected type for the config key in %s is %T, but a %T was provided instead", co.Name, key, co.ConfigKey)
}

func SetConfigOptionLogLevel(co *config.ConfigOption) error {
	logLevelStr := viper.GetString(co.Name)
	logLevel, err := logrus.ParseLevel(logLevelStr)
	if err != nil {
		return fmt.Errorf("couldn't parse log level in %s: %w", co.Name, err)
	}

	key, ok := co.ConfigKey.(*logrus.Level)
	if !ok {
		return fmt.Errorf("%s configKey has an invalid type %T", co.Name, co.ConfigKey)
	}
	*key = logLevel

	// The logger starts at TRACE so config parsing itself is visible; apply the
	// configured level whether it came from a flag, an env var, or the default.
	log.DefaultLogger.SetLevel(*key)
	if config.IsExplicitlySet(co) {
		log.Debugf("Setting log level to: %s", logLevel)
	} else {
		log.Debugf("Using default log level: %s", logLevel)
	}

	return nil
}

func SetConfigOptionStellarPublicKeyList(co *config.ConfigOption) error {
	publicKeysStr := viper.GetString(co.Name)
	publicKeysStr = strings.TrimSpace(publicKeysStr)
	if publicKeysStr == "" {
		if co.Required {
			return fmt.Errorf("no public keys provided in %s", co.Name)
		}
		// If not required and empty, set to empty slice
		key, ok := co.ConfigKey.(*[]string)
		if !ok {
			return unexpectedTypeError(key, co)
		}
		*key = []string{}
		return nil
	}
	publicKeysStr = strings.ReplaceAll(publicKeysStr, " ", "")
	publicKeys := strings.Split(publicKeysStr, ",")

	m := make(map[string]struct{})
	for _, publicKey := range publicKeys {
		_, err := keypair.ParseAddress(publicKey)
		if err != nil {
			return fmt.Errorf("validating public key %q in %s: %w", publicKey, co.Name, err)
		}
		m[publicKey] = struct{}{}
	}

	key, ok := co.ConfigKey.(*[]string)
	if !ok {
		return unexpectedTypeError(key, co)
	}

	pbks := make([]string, 0, len(m))
	for k := range m {
		pbks = append(pbks, k)
	}
	sort.Strings(pbks)
	*key = pbks

	return nil
}

// SetConfigOptionDuration parses a Go duration string (e.g. "5m", "10s") from the CLI flag
// or environment variable and stores the result in a *time.Duration ConfigKey.
func SetConfigOptionDuration(co *config.ConfigOption) error {
	durationStr := viper.GetString(co.Name)
	if durationStr == "" {
		return fmt.Errorf("%s cannot be empty", co.Name)
	}

	d, err := time.ParseDuration(durationStr)
	if err != nil {
		return fmt.Errorf("couldn't parse duration in %s: %w", co.Name, err)
	}

	key, ok := co.ConfigKey.(*time.Duration)
	if !ok {
		return fmt.Errorf("%s configKey has an invalid type %T", co.Name, co.ConfigKey)
	}
	*key = d

	return nil
}
