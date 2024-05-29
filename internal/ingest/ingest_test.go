package ingest

import (
	"testing"

	"github.com/stellar/go/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCaptiveCoreConfig(t *testing.T) {
	t.Run("testnet_success", func(t *testing.T) {
		config, err := getCaptiveCoreConfig(Configs{
			NetworkPassphrase:    network.TestNetworkPassphrase,
			CaptiveCoreBinPath:   "/bin/path",
			CaptiveCoreConfigDir: "./config",
		})

		require.NoError(t, err)
		assert.Equal(t, "/bin/path", config.BinaryPath)
		assert.Equal(t, network.TestNetworkPassphrase, config.NetworkPassphrase)
		assert.Equal(t, network.TestNetworkhistoryArchiveURLs, config.HistoryArchiveURLs)
		assert.Equal(t, true, config.UseDB)
		assert.NotNil(t, config.Toml)
	})

	t.Run("pubnet_success", func(t *testing.T) {
		config, err := getCaptiveCoreConfig(Configs{
			NetworkPassphrase:    network.PublicNetworkPassphrase,
			CaptiveCoreBinPath:   "/bin/path",
			CaptiveCoreConfigDir: "./config",
		})

		require.NoError(t, err)
		assert.Equal(t, "/bin/path", config.BinaryPath)
		assert.Equal(t, network.PublicNetworkPassphrase, config.NetworkPassphrase)
		assert.Equal(t, network.PublicNetworkhistoryArchiveURLs, config.HistoryArchiveURLs)
		assert.Equal(t, true, config.UseDB)
		assert.NotNil(t, config.Toml)
	})

	t.Run("unknown_network", func(t *testing.T) {
		_, err := getCaptiveCoreConfig(Configs{
			NetworkPassphrase:    "Invalid SDF Network ; May 2024",
			CaptiveCoreBinPath:   "/bin/path",
			CaptiveCoreConfigDir: "./config",
		})

		assert.ErrorContains(t, err, "unknown network: Invalid SDF Network ; May 2024")
	})

	t.Run("invalid_config_file", func(t *testing.T) {
		_, err := getCaptiveCoreConfig(Configs{
			NetworkPassphrase:    network.TestNetworkPassphrase,
			CaptiveCoreBinPath:   "/bin/path",
			CaptiveCoreConfigDir: "./invalid/path",
		})

		assert.ErrorContains(t, err, "captive core configuration file not found in invalid/path/stellar-core_testnet.cfg")
	})
}
