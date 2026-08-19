package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateRepairOpts(t *testing.T) {
	const (
		contractAddress = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		accountAddress  = "GCYNTH5HDQRNIQ3BSSYPWFO5AHH5ERVZ32C37QRXT6TXK3OJFFOIVXDE"
	)

	testCases := []struct {
		name        string
		opts        repairOpts
		expectedErr string
	}{
		{
			name:        "🔴no protocol",
			opts:        repairOpts{all: true, concurrency: 4},
			expectedErr: "--protocol is required",
		},
		{
			name:        "🔴no scope",
			opts:        repairOpts{protocolID: "SEP41", concurrency: 4},
			expectedErr: "specify --contract and/or --account, or --all to verify every indexed pair",
		},
		{
			name:        "🔴all combined with contract",
			opts:        repairOpts{protocolID: "SEP41", all: true, contractAddress: contractAddress, concurrency: 4},
			expectedErr: "--all cannot be combined with --contract or --account",
		},
		{
			name:        "🔴contract is not a contract address",
			opts:        repairOpts{protocolID: "SEP41", contractAddress: accountAddress, concurrency: 4},
			expectedErr: `is not a valid contract address`,
		},
		{
			name:        "🔴account is not a stellar address",
			opts:        repairOpts{protocolID: "SEP41", accountAddress: "not-an-address", concurrency: 4},
			expectedErr: `is not a valid account or contract address`,
		},
		{
			name:        "🔴concurrency below one",
			opts:        repairOpts{protocolID: "SEP41", all: true, concurrency: 0},
			expectedErr: "--concurrency must be at least 1",
		},
		{
			name: "🟢all",
			opts: repairOpts{protocolID: "SEP41", all: true, concurrency: 4},
		},
		{
			name: "🟢contract and account",
			opts: repairOpts{protocolID: "SEP41", contractAddress: contractAddress, accountAddress: accountAddress, concurrency: 1},
		},
		{
			name: "🟢contract holder account",
			opts: repairOpts{protocolID: "SEP41", accountAddress: contractAddress, concurrency: 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRepairOpts(&tc.opts)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}
