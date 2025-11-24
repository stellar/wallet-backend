package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLedgerBackend_RPC(t *testing.T) {
	cfg := Configs{
		LedgerBackendType: LedgerBackendTypeRPC,
		RPCURL:            "http://localhost:8000/rpc",
		GetLedgersLimit:   100,
	}

	backend, err := NewLedgerBackend(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, backend)
}

func TestNewLedgerBackend_InvalidType(t *testing.T) {
	cfg := Configs{
		LedgerBackendType: "invalid",
	}

	backend, err := NewLedgerBackend(context.Background(), cfg)
	assert.Error(t, err)
	assert.Nil(t, backend)
	assert.Contains(t, err.Error(), "unsupported ledger backend type")
}

func TestNewRPCLedgerBackend(t *testing.T) {
	testCases := []struct {
		name        string
		cfg         Configs
		expectError bool
	}{
		{
			name: "valid_config_small_buffer",
			cfg: Configs{
				RPCURL:          "http://localhost:8000/rpc",
				GetLedgersLimit: 10,
			},
			expectError: false,
		},
		{
			name: "valid_config_large_buffer",
			cfg: Configs{
				RPCURL:          "http://localhost:8000/rpc",
				GetLedgersLimit: 1000,
			},
			expectError: false,
		},
		{
			name: "zero_buffer_size",
			cfg: Configs{
				RPCURL:          "http://localhost:8000/rpc",
				GetLedgersLimit: 0,
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			backend, err := newRPCLedgerBackend(tc.cfg)
			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, backend)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, backend)
			}
		})
	}
}

func TestLoadDatastoreBackendConfig_EmptyPath(t *testing.T) {
	cfg, err := loadDatastoreBackendConfig("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "datastore config file path is required")
	assert.Equal(t, StorageBackendConfig{}, cfg)
}

func TestLoadDatastoreBackendConfig_InvalidPath(t *testing.T) {
	cfg, err := loadDatastoreBackendConfig("/nonexistent/path/config.toml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "loading datastore config file")
	assert.Equal(t, StorageBackendConfig{}, cfg)
}

func TestLoadDatastoreBackendConfig_InvalidTOML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.toml")

	err := os.WriteFile(configPath, []byte("invalid toml content [[["), 0644)
	require.NoError(t, err)

	cfg, err := loadDatastoreBackendConfig(configPath)
	assert.Error(t, err)
	assert.Equal(t, StorageBackendConfig{}, cfg)
}

func TestLoadDatastoreBackendConfig_Success(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	validTOML := `
[datastore_config]
network_passphrase = "Test SDF Network ; September 2015"

[buffered_storage_backend_config]
ledger_batch_size = 64
num_ledger_batches_to_retain = 2
`

	err := os.WriteFile(configPath, []byte(validTOML), 0644)
	require.NoError(t, err)

	cfg, err := loadDatastoreBackendConfig(configPath)
	assert.NoError(t, err)
	assert.Equal(t, uint32(64), cfg.BufferedStorageBackendConfig.LedgerBatchSize)
	assert.Equal(t, uint32(2), cfg.BufferedStorageBackendConfig.NumLedgerBatchesToRetain)
}

func TestLoadDatastoreBackendConfig_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "empty.toml")

	err := os.WriteFile(configPath, []byte(""), 0644)
	require.NoError(t, err)

	cfg, err := loadDatastoreBackendConfig(configPath)
	assert.NoError(t, err)
	assert.NotNil(t, cfg)
}
