package ingest

import (
	"context"
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

func TestDatastoreConfig_dataStoreConfig(t *testing.T) {
	const passphrase = "Test SDF Network ; September 2015"

	t.Run("bucket only: region/endpoint omitted, schema from manifest", func(t *testing.T) {
		got := DatastoreConfig{BucketPath: "my-bucket/ledgers"}.dataStoreConfig(passphrase)

		assert.Equal(t, "S3", got.Type)
		assert.Equal(t, passphrase, got.NetworkPassphrase)
		assert.Equal(t, map[string]string{"destination_bucket_path": "my-bucket/ledgers"}, got.Params)
		// Zero schema means "read from the datastore manifest".
		assert.Equal(t, uint32(0), got.Schema.LedgersPerFile)
		assert.Equal(t, uint32(0), got.Schema.FilesPerPartition)
	})

	t.Run("region, endpoint and inline schema set", func(t *testing.T) {
		got := DatastoreConfig{
			BucketPath:        "ledgers",
			Region:            "us-east-1",
			EndpointURL:       "http://minio:9000",
			LedgersPerFile:    1,
			FilesPerPartition: 1,
		}.dataStoreConfig(passphrase)

		assert.Equal(t, map[string]string{
			"destination_bucket_path": "ledgers",
			"region":                  "us-east-1",
			"endpoint_url":            "http://minio:9000",
		}, got.Params)
		assert.Equal(t, uint32(1), got.Schema.LedgersPerFile)
		assert.Equal(t, uint32(1), got.Schema.FilesPerPartition)
	})

	t.Run("empty region and endpoint are omitted from params", func(t *testing.T) {
		got := DatastoreConfig{BucketPath: "b"}.dataStoreConfig(passphrase)

		require.NotContains(t, got.Params, "region")
		require.NotContains(t, got.Params, "endpoint_url")
	})
}
