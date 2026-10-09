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

// setPositiveFloat parses a float option and rejects zero and negative values.
func setPositiveFloat(co *config.ConfigOption) error {
	v, err := strconv.ParseFloat(viper.GetString(co.Name), 64)
	if err != nil {
		return fmt.Errorf("couldn't parse number in %s: %w", co.Name, err)
	}
	if v <= 0 {
		return fmt.Errorf("%s must be greater than 0, got %v", co.Name, v)
	}
	key, ok := co.ConfigKey.(*float64)
	if !ok {
		return fmt.Errorf("%s configKey has an invalid type %T", co.Name, co.ConfigKey)
	}
	*key = v
	return nil
}

// PricesOptions returns the config options for token price serving.
func PricesOptions(snapshotInterval *time.Duration, minVolume24hUSD *float64, maxStaleness *time.Duration) config.ConfigOptions {
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
			Name:           "prices-min-volume-24h-usd",
			Usage:          "Minimum 24-hour USD trading volume for a token's price to be served.",
			OptType:        types.String,
			CustomSetValue: setPositiveFloat,
			ConfigKey:      minVolume24hUSD,
			FlagDefault:    strconv.FormatFloat(serve.DefaultPricesMinVolume24hUSD, 'f', -1, 64),
		},
		{
			Name:           "prices-max-staleness",
			Usage:          "Maximum age of a token's last trade for its price to be served (Go duration string, e.g. \"168h\").",
			OptType:        types.String,
			CustomSetValue: setPositiveDuration,
			ConfigKey:      maxStaleness,
			FlagDefault:    serve.DefaultPricesMaxStaleness.String(),
		},
	}
}
