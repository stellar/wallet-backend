package utils

import (
	"fmt"
	"go/types"
	"strconv"
	"time"

	"github.com/spf13/viper"
	"github.com/stellar/go-stellar-sdk/support/config"

	"github.com/stellar/wallet-backend/internal/serve"
)

// setPositiveDuration parses a duration option and rejects zero and negative values.
func setPositiveDuration(co *config.ConfigOption) error {
	if err := SetConfigOptionDuration(co); err != nil {
		return err
	}
	if d := *co.ConfigKey.(*time.Duration); d <= 0 {
		return fmt.Errorf("%s must be greater than 0, got %s", co.Name, d)
	}
	return nil
}

// setErrorFraction parses a float option that must be greater than 0 and at most 1.
func setErrorFraction(co *config.ConfigOption) error {
	v, err := strconv.ParseFloat(viper.GetString(co.Name), 64)
	if err != nil {
		return fmt.Errorf("couldn't parse number in %s: %w", co.Name, err)
	}
	if v <= 0 || v > 1 {
		return fmt.Errorf("%s must be greater than 0 and at most 1, got %v", co.Name, v)
	}
	key, ok := co.ConfigKey.(*float64)
	if !ok {
		return fmt.Errorf("%s configKey has an invalid type %T", co.Name, co.ConfigKey)
	}
	*key = v
	return nil
}

// PricesOptions returns the config options for token price serving.
func PricesOptions(snapshotInterval *time.Duration, maxError *float64) config.ConfigOptions {
	return config.ConfigOptions{
		{
			Name:           "prices-snapshot-interval",
			Usage:          "How often the in-memory token price snapshot is reloaded (Go duration string, e.g. \"5s\").",
			OptType:        types.String,
			CustomSetValue: setPositiveDuration,
			ConfigKey:      snapshotInterval,
			FlagDefault:    serve.DefaultPricesSnapshotInterval.String(),
		},
		{
			Name:           "prices-max-error",
			Usage:          "Largest estimated relative error (0.05 = 5%) at which a token price is served.",
			OptType:        types.String,
			CustomSetValue: setErrorFraction,
			ConfigKey:      maxError,
			FlagDefault:    strconv.FormatFloat(serve.DefaultPricesMaxError, 'f', -1, 64),
		},
	}
}
