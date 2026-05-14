package services

import (
	"fmt"
	"sort"
	"sync"
)

// processorRegistryMu guards concurrent access to processorRegistry.
var processorRegistryMu sync.RWMutex

// processorRegistry holds factory functions keyed by protocol ID. Factories
// receive a ProtocolDeps so per-protocol packages can avoid package-level
// mutable state — no SetDependencies pattern required.
var processorRegistry = map[string]func(ProtocolDeps) ProtocolProcessor{}

// RegisterProcessor registers a processor factory for a protocol ID. Called
// from per-protocol package init() functions.
func RegisterProcessor(protocolID string, factory func(ProtocolDeps) ProtocolProcessor) {
	processorRegistryMu.Lock()
	defer processorRegistryMu.Unlock()
	processorRegistry[protocolID] = factory
}

// GetProcessor returns the processor factory for a protocol ID, if registered.
func GetProcessor(protocolID string) (func(ProtocolDeps) ProtocolProcessor, bool) {
	processorRegistryMu.RLock()
	defer processorRegistryMu.RUnlock()
	factory, ok := processorRegistry[protocolID]
	return factory, ok
}

// GetAllProcessorIDs returns the registered protocol IDs in sorted order so
// callers that iterate the full set get deterministic ordering.
func GetAllProcessorIDs() []string {
	processorRegistryMu.RLock()
	defer processorRegistryMu.RUnlock()
	ids := make([]string, 0, len(processorRegistry))
	for id := range processorRegistry {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// BuildProcessors materializes processors for the given protocol IDs from the
// registry. Returns an error if any ID has no registered factory.
func BuildProcessors(deps ProtocolDeps, protocolIDs []string) ([]ProtocolProcessor, error) {
	out := make([]ProtocolProcessor, 0, len(protocolIDs))
	for _, pid := range protocolIDs {
		factory, ok := GetProcessor(pid)
		if !ok {
			return nil, fmt.Errorf("no processor registered for protocol %q", pid)
		}
		p := factory(deps)
		if p == nil {
			return nil, fmt.Errorf("processor factory for protocol %q returned nil", pid)
		}
		out = append(out, p)
	}
	return out, nil
}

// resetProcessorRegistry replaces the registry map while holding the write
// lock. Intended for tests only.
func resetProcessorRegistry(m map[string]func(ProtocolDeps) ProtocolProcessor) {
	processorRegistryMu.Lock()
	defer processorRegistryMu.Unlock()
	processorRegistry = m
}
