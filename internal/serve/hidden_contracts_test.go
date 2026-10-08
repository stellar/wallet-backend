package serve

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const cometContractID = "CAS3FL6TLZKDGGSISDBWGGPXT3NRR4DYTZD7YOD3HMYO6LTJUVGRVEAM"

func testContractAddress(t *testing.T, fill byte) (string, [32]byte) {
	t.Helper()
	var raw [32]byte
	for i := range raw {
		raw[i] = fill
	}
	addr, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	require.NoError(t, err)
	return addr, raw
}

func writeHiddenContractsFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hidden.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoadHiddenContracts(t *testing.T) {
	addrA, _ := testContractAddress(t, 0x01)
	addrB, _ := testContractAddress(t, 0x02)

	t.Run("embedded default", func(t *testing.T) {
		c, err := LoadHiddenContracts("")
		require.NoError(t, err)
		require.Equal(t, []string{cometContractID}, c.ContractAddresses("SEP41"))
	})

	t.Run("file replaces the default", func(t *testing.T) {
		path := writeHiddenContractsFile(t, "hidden_contracts:\n"+
			"  - contract_id: "+addrA+"\n    protocol: SEP41\n    reason: a\n"+
			"  - contract_id: "+addrB+"\n    protocol: SEP41\n    reason: b\n")
		c, err := LoadHiddenContracts(path)
		require.NoError(t, err)
		require.Equal(t, []string{addrA, addrB}, c.ContractAddresses("SEP41"))
	})

	t.Run("the same contract may be listed once per protocol", func(t *testing.T) {
		path := writeHiddenContractsFile(t, "hidden_contracts:\n"+
			"  - contract_id: "+addrA+"\n    protocol: SEP41\n    reason: a\n"+
			"  - contract_id: "+addrA+"\n    protocol: SEP41\n    reason: b\n")
		_, err := LoadHiddenContracts(path)
		require.ErrorContains(t, err, "listed more than once for SEP41")
	})

	t.Run("empty list", func(t *testing.T) {
		c, err := LoadHiddenContracts(writeHiddenContractsFile(t, "hidden_contracts: []\n"))
		require.NoError(t, err)
		require.Empty(t, c.ContractAddresses("SEP41"))
		require.Empty(t, c.TokenRanges())
		require.Empty(t, c.ContractAddresses("SEP41"))
	})

	invalid := []struct {
		name    string
		content string
	}{
		{"invalid address", "hidden_contracts:\n  - contract_id: GNOTACONTRACT\n    protocol: SEP41\n    reason: x\n"},
		{"missing reason", "hidden_contracts:\n  - contract_id: " + addrA + "\n    protocol: SEP41\n    reason: \"  \"\n"},
		{"missing protocol", "hidden_contracts:\n  - contract_id: " + addrA + "\n    reason: a\n"},
		{"unknown protocol", "hidden_contracts:\n  - contract_id: " + addrA + "\n    protocol: NOPE\n    reason: a\n"},
		{"misspelled field", "hidden_contracts:\n  - contract_id: " + addrA + "\n    protcol: SEP41\n    reason: a\n"},
		{"misspelled top-level key", "hidden_contract:\n  - contract_id: " + addrA + "\n    protocol: SEP41\n    reason: a\n"},
		{"second YAML document", "hidden_contracts:\n  - contract_id: " + addrA + "\n    protocol: SEP41\n    reason: a\n---\nhidden_contracts: []\n"},
		{"empty file", ""},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadHiddenContracts(writeHiddenContractsFile(t, tc.content))
			require.Error(t, err)
		})
	}
}

func TestHiddenContractsForms(t *testing.T) {
	addr, raw := testContractAddress(t, 0x07)
	c, err := LoadHiddenContracts(writeHiddenContractsFile(t,
		"hidden_contracts:\n  - contract_id: "+addr+"\n    protocol: SEP41\n    reason: a\n"))
	require.NoError(t, err)

	ranges := c.TokenRanges()
	require.Len(t, ranges, 1)
	require.Len(t, ranges[0].TokenID, 33)
	require.Equal(t, byte(strkey.VersionByteContract), ranges[0].TokenID[0])
	require.Equal(t, hex.EncodeToString(raw[:]), hex.EncodeToString(ranges[0].TokenID[1:]))
	require.Equal(t, types.StateChangeOrdinalBaseSEP41, ranges[0].FromID)
	require.Equal(t, types.StateChangeOrdinalBaseSEP41+types.StateChangeOrdinalNamespaceWidth, ranges[0].ToID)

	require.Equal(t, []string{addr}, c.ContractAddresses("SEP41"))
	require.Empty(t, c.ContractAddresses("OTHER"))
	require.Equal(t, []string{addr + " (SEP41)"}, c.Labels())
}

func TestApplyHiddenContracts(t *testing.T) {
	addr, _ := testContractAddress(t, 0x07)
	c, err := LoadHiddenContracts(writeHiddenContractsFile(t,
		"hidden_contracts:\n  - contract_id: "+addr+"\n    protocol: SEP41\n    reason: a\n"))
	require.NoError(t, err)

	models := &data.Models{StateChanges: &data.StateChangeModel{}}
	applyHiddenContracts(models, nil, nil, c)

	require.Equal(t, c.TokenRanges(), models.StateChanges.HiddenTokenRanges)
	balances, ok := models.SEP41.Balances.(*sep41data.BalanceModel)
	require.True(t, ok)
	require.Equal(t, []string{addr}, balances.HiddenContracts)
	allowances, ok := models.SEP41.Allowances.(*sep41data.AllowanceModel)
	require.True(t, ok)
	require.Equal(t, []string{addr}, allowances.HiddenContracts)
}
