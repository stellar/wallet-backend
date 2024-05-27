package utils

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/spf13/viper"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/config"
	"github.com/stellar/wallet-backend/internal/ingest"
)

func unexpectedTypeError(key any, co *config.ConfigOption) error {
	return fmt.Errorf("the expected type for the config key in %s is %T, but a %T was provided instead", co.Name, key, co.ConfigKey)
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

func SetConfigOptionCaptiveCoreBinPath(co *config.ConfigOption) error {
	binPath := viper.GetString(co.Name)

	fileInfo, err := os.Stat(binPath)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("binary file %s does not exist", binPath)
	}

	if fileInfo.IsDir() {
		return fmt.Errorf("binary file path %s is a directory, not a file", binPath)
	}

	key, ok := co.ConfigKey.(*string)
	if !ok {
		return unexpectedTypeError(key, co)
	}
	*key = binPath

	return nil
}

func SetConfigOptionCaptiveCoreConfigDir(co *config.ConfigOption) error {
	dirPath := viper.GetString(co.Name)

	fileInfo, err := os.Stat(dirPath)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("captive core configuration files dir %s does not exist", dirPath)
	}

	if !fileInfo.IsDir() {
		return fmt.Errorf("captive core configuration files dir %s is not a directory", dirPath)
	}

	testnetConfigFile := path.Join(dirPath, ingest.ConfigFileNameTestnet)
	if _, err := os.Stat(testnetConfigFile); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("captive core testnet configuration file %s does not exist in dir %s", testnetConfigFile, dirPath)
	}

	pubnetConfigFile := path.Join(dirPath, ingest.ConfigFileNamePubnet)
	if _, err := os.Stat(pubnetConfigFile); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("captive core pubnet configuration file %s does not exist in dir %s", pubnetConfigFile, dirPath)
	}

	key, ok := co.ConfigKey.(*string)
	if !ok {
		return unexpectedTypeError(key, co)
	}
	*key = dirPath

	return nil
}
