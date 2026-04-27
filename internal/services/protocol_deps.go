package services

import (
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// ProtocolDeps carries the runtime objects each protocol's validator and
// processor factories may need. Built once by the cmd/ingest layer from
// process-wide singletons and passed through the registry to per-protocol
// factories. Per-protocol packages pull only the fields they care about — the
// shape stays generic, so adding a new protocol does not require new wiring
// in cmd/ or internal/ingest/.
//
// RPCService is allowed to be nil for command paths that do not have RPC
// configured (e.g. the datastore-backed protocol-migrate run). Factories that
// need RPC must check for nil and degrade or error accordingly.
type ProtocolDeps struct {
	NetworkPassphrase       string
	Models                  *data.Models
	RPCService              RPCService
	ContractMetadataService ContractMetadataService
	MetricsService          *metrics.Metrics
}
