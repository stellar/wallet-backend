package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/ingest"
)

func TestValidateHistoryLedgerBackend(t *testing.T) {
	testCases := []struct {
		name        string
		backendType string
		rpcURL      string
		bucketPath  string
		wantErr     string
	}{
		{name: "rpc_with_url", backendType: "rpc", rpcURL: "http://rpc"},
		{name: "rpc_without_url", backendType: "rpc", wantErr: "--rpc-url is required when --ledger-backend-type=rpc"},
		{name: "datastore_with_bucket", backendType: "datastore", bucketPath: "bucket/path"},
		{name: "datastore_without_bucket", backendType: "datastore", rpcURL: "http://rpc", wantErr: "--datastore-bucket-path is required when --ledger-backend-type=datastore"},
		{name: "unknown_type", backendType: "s3", rpcURL: "http://rpc", wantErr: `invalid --ledger-backend-type "s3", must be 'rpc' or 'datastore'`},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateHistoryLedgerBackend(tc.backendType, tc.rpcURL, ingest.DatastoreConfig{BucketPath: tc.bucketPath})
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.wantErr)
		})
	}
}
