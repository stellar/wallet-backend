package utils

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/config"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
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

	// Log for debugging
	if config.IsExplicitlySet(co) {
		log.Debugf("Setting log level to: %s", logLevel)
		log.DefaultLogger.SetLevel(*key)
	} else {
		log.Debugf("Using default log level: %s", logLevel)
	}

	return nil
}

func SetConfigOptionStellarPublicKey(co *config.ConfigOption) error {
	publicKey := viper.GetString(co.Name)

	kp, err := keypair.ParseAddress(publicKey)
	if err != nil {
		return fmt.Errorf("validating public key in %s: %w", co.Name, err)
	}

	key, ok := co.ConfigKey.(*string)
	if !ok {
		return unexpectedTypeError(key, co)
	}
	*key = kp.Address()

	return nil
}

func SetConfigOptionStellarPrivateKey(co *config.ConfigOption) error {
	privateKey := viper.GetString(co.Name)

	if privateKey == "" && !co.Required {
		return nil
	}

	isValid := strkey.IsValidEd25519SecretSeed(privateKey)
	if !isValid {
		return fmt.Errorf("invalid private key provided in %s", co.Name)
	}

	key, ok := co.ConfigKey.(*string)
	if !ok {
		return unexpectedTypeError(key, co)
	}
	*key = privateKey

	return nil
}

func SetConfigOptionAssets(co *config.ConfigOption) error {
	assetsJSON := viper.GetString(co.Name)

	if assetsJSON == "" {
		return fmt.Errorf("assets cannot be empty")
	}

	var assets []entities.Asset
	err := json.NewDecoder(strings.NewReader(assetsJSON)).Decode(&assets)
	if err != nil {
		return fmt.Errorf("decoding assets JSON: %w", err)
	}

	key, ok := co.ConfigKey.(*[]entities.Asset)
	if !ok {
		return unexpectedTypeError(key, co)
	}
	*key = assets

	return nil
}

func SetConfigOptionSignatureClientProvider(co *config.ConfigOption) error {
	scType := viper.GetString(co.Name)

	scType = strings.TrimSpace(scType)
	if scType == "" {
		return fmt.Errorf("%s cannot be empty", co.Name)
	}

	t := signing.SignatureClientType(scType)
	if !t.IsValid() {
		return fmt.Errorf("invalid %s value provided. expected: ENV or KMS", co.Name)
	}

	key, ok := co.ConfigKey.(*signing.SignatureClientType)
	if !ok {
		return unexpectedTypeError(key, co)
	}
	*key = t

	return nil
}
