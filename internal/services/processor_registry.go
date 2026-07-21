package services

import (
	"fmt"
	"sort"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// processorRegistry holds factory functions keyed by protocol ID. Factories
// receive a ProtocolDeps so per-protocol packages can avoid package-level
// mutable state — no SetDependencies pattern required.
//
// Writes happen only from per-protocol package init() functions; reads happen
// only after main() starts. Adding a new protocol requires a binary rebuild +
// restart — there is no runtime registration path. Go's init() machinery
// serializes all init writes on a single goroutine completing before main(),
// which is what makes access here safe without synchronization. If a runtime
// registration path is ever introduced (plugin.Open, admin endpoint, etc.),
// reintroduce synchronization here and reconsider the snapshot held by
// ingestService.protocolProcessors (which is built once at startup and never
// re-reads the registry).
var processorRegistry = map[string]func(ProtocolDeps) ProtocolProcessor{}

// RegisterProcessor registers a processor factory for a protocol ID. Called
// from per-protocol package init() functions.
func RegisterProcessor(protocolID string, factory func(ProtocolDeps) ProtocolProcessor) {
	processorRegistry[protocolID] = factory
}

// GetProcessor returns the processor factory for a protocol ID, if registered.
func GetProcessor(protocolID string) (func(ProtocolDeps) ProtocolProcessor, bool) {
	factory, ok := processorRegistry[protocolID]
	return factory, ok
}

// GetAllProcessorIDs returns the registered protocol IDs in sorted order so
// callers that iterate the full set get deterministic ordering.
func GetAllProcessorIDs() []string {
	ids := make([]string, 0, len(processorRegistry))
	for id := range processorRegistry {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// BuildProcessors materializes processors for the given protocol IDs from the
// registry. Returns an error if any ID has no registered factory.
//
// It also validates the state_change_id namespace bases of the built set: every
// base must be positive and a multiple of types.StateChangeOrdinalNamespaceWidth
// (base 0 is the main indexer's sub-namespaced region and is off-limits to
// protocol processors), and no two processors may share a base. Both boot entry
// points (internal/ingest and cmd/protocol_migrate) funnel through here, so a
// stale or duplicated base copied into a new processor fails fast at startup
// rather than silently colliding state_change_ids on-disk.
func BuildProcessors(deps ProtocolDeps, protocolIDs []string) ([]ProtocolProcessor, error) {
	out := make([]ProtocolProcessor, 0, len(protocolIDs))
	protocolIDByBase := make(map[int64]string, len(protocolIDs))
	for _, pid := range protocolIDs {
		factory, ok := GetProcessor(pid)
		if !ok {
			return nil, fmt.Errorf("no processor registered for protocol %q", pid)
		}
		p := factory(deps)
		if p == nil {
			return nil, fmt.Errorf("processor factory for protocol %q returned nil", pid)
		}

		base := p.StateChangeOrdinalBase()
		if base <= 0 || base%types.StateChangeOrdinalNamespaceWidth != 0 {
			return nil, fmt.Errorf("protocol %q has invalid state_change_id base %d: "+
				"must be a positive multiple of %d", p.ProtocolID(), base, types.StateChangeOrdinalNamespaceWidth)
		}
		if other, dup := protocolIDByBase[base]; dup {
			return nil, fmt.Errorf("protocols %q and %q share state_change_id base %d",
				other, p.ProtocolID(), base)
		}
		protocolIDByBase[base] = p.ProtocolID()

		out = append(out, p)
	}
	return out, nil
}
