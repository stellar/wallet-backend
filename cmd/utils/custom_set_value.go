package utils

import (
	"fmt"

	"github.com/spf13/viper"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/config"
)

func SetConfigOptionStellarPublicKey(co *config.ConfigOption) error {
	publicKey := viper.GetString(co.Name)

	kp, err := keypair.ParseAddress(publicKey)
	if err != nil {
		return fmt.Errorf("error validating public key in %s: %w", co.Name, err)
	}

	key, ok := co.ConfigKey.(*string)
	if !ok {
		return fmt.Errorf("the expected type for the config key in %s is a string, but a %T was provided instead", co.Name, co.ConfigKey)
	}
	*key = kp.Address()

	return nil
}
