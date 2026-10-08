package serve

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/strkey"
	"gopkg.in/yaml.v3"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services/sep41"
)

//go:embed hidden_contracts.yaml
var defaultHiddenContractsYAML []byte

// HiddenContracts is the set of Soroban token contracts the API does not
// return. Ingestion keeps processing them; only reads skip them. It is loaded
// once at startup.
type HiddenContracts struct {
	entries []hiddenContract // in file order
}

// hiddenContract is one list entry: the contract and the protocol whose
// processor's rows for it are hidden. The same contract may appear once per
// protocol.
type hiddenContract struct {
	address  string
	protocol string
}

type hiddenContractsFile struct {
	HiddenContracts *[]struct {
		ContractID string `yaml:"contract_id"`
		Protocol   string `yaml:"protocol"`
		Reason     string `yaml:"reason"`
		Added      string `yaml:"added"` // informational; not validated
	} `yaml:"hidden_contracts"`
}

// LoadHiddenContracts reads the list from path, or from the copy embedded in
// the binary when path is empty. A file replaces the embedded list entirely.
// Unknown keys and a missing top-level key are errors, so a typo cannot
// silently widen or empty the list.
func LoadHiddenContracts(path string) (*HiddenContracts, error) {
	raw := defaultHiddenContractsYAML
	if path != "" {
		var err error
		raw, err = os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading hidden contracts file %s: %w", path, err)
		}
	}

	var file hiddenContractsFile
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("parsing hidden contracts: %w", err)
	}
	if file.HiddenContracts == nil {
		return nil, fmt.Errorf("parsing hidden contracts: the hidden_contracts key is missing (use `hidden_contracts: []` for none)")
	}
	// A second YAML document would be ignored; refuse it so nothing after the
	// first document is silently dropped.
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parsing hidden contracts: more than one YAML document in the file")
	}

	c := &HiddenContracts{}
	seen := make(map[hiddenContract]struct{}, len(*file.HiddenContracts))
	for i, entry := range *file.HiddenContracts {
		n := i + 1
		if _, err := strkey.Decode(strkey.VersionByteContract, entry.ContractID); err != nil {
			return nil, fmt.Errorf("hidden contract %d: invalid contract_id %q: %w", n, entry.ContractID, err)
		}
		if strings.TrimSpace(entry.Reason) == "" {
			return nil, fmt.Errorf("hidden contract %d (%s): reason is empty", n, entry.ContractID)
		}
		if entry.Protocol == "" {
			return nil, fmt.Errorf("hidden contract %d (%s): protocol is required", n, entry.ContractID)
		}
		if _, known := types.StateChangeOrdinalBaseByProtocol[entry.Protocol]; !known {
			return nil, fmt.Errorf("hidden contract %d (%s): unknown protocol %q", n, entry.ContractID, entry.Protocol)
		}
		key := hiddenContract{address: entry.ContractID, protocol: entry.Protocol}
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("hidden contract %d (%s): listed more than once for %s", n, entry.ContractID, entry.Protocol)
		}
		seen[key] = struct{}{}
		c.entries = append(c.entries, key)
	}
	return c, nil
}

// Labels returns one "address (protocol)" string per entry, for the startup log.
func (c *HiddenContracts) Labels() []string {
	labels := make([]string, 0, len(c.entries))
	for _, e := range c.entries {
		labels = append(labels, e.address+" ("+e.protocol+")")
	}
	return labels
}

// tokenID is the form state_changes.token_id stores: the 33-byte address
// (version byte followed by the 32-byte hash). Addresses were validated at
// load, so decoding cannot fail here.
func tokenID(address string) []byte {
	hash := strkey.MustDecode(strkey.VersionByteContract, address)
	return append([]byte{byte(strkey.VersionByteContract)}, hash...)
}

// TokenRanges returns, per entry, the token id and the entry's protocol
// state_change_id range.
func (c *HiddenContracts) TokenRanges() []data.HiddenTokenRange {
	ranges := make([]data.HiddenTokenRange, 0, len(c.entries))
	for _, e := range c.entries {
		base := types.StateChangeOrdinalBaseByProtocol[e.protocol]
		ranges = append(ranges, data.HiddenTokenRange{TokenID: tokenID(e.address), FromID: base, ToID: base + types.StateChangeOrdinalNamespaceWidth})
	}
	return ranges
}

// ContractAddresses returns the C-addresses hidden from the given protocol.
func (c *HiddenContracts) ContractAddresses(protocolID string) []string {
	addrs := make([]string, 0, len(c.entries))
	for _, e := range c.entries {
		if e.protocol == protocolID {
			addrs = append(addrs, e.address)
		}
	}
	return addrs
}

// applyHiddenContracts makes every read on models skip the hidden contracts.
// The SEP-41 models are rebuilt because their hidden set is fixed at
// construction.
func applyHiddenContracts(models *data.Models, pool *pgxpool.Pool, dbMetrics *metrics.DBMetrics, hidden *HiddenContracts) {
	models.StateChanges.HiddenTokenRanges = hidden.TokenRanges()
	models.SEP41 = sep41data.NewModels(pool, dbMetrics, hidden.ContractAddresses(sep41.ProtocolID))
}
