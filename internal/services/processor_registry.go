package services

import "sync"

// processorRegistryMu guards concurrent access to processorRegistry.
var processorRegistryMu sync.RWMutex

// processorRegistry holds factory functions keyed by protocol ID.
var processorRegistry = map[string]func() ProtocolProcessor{}

// RegisterProcessor registers a processor factory for a protocol ID.
func RegisterProcessor(protocolID string, factory func() ProtocolProcessor) {
	processorRegistryMu.Lock()
	defer processorRegistryMu.Unlock()
	processorRegistry[protocolID] = factory
}

// GetProcessor returns the processor factory for a protocol ID, if registered.
func GetProcessor(protocolID string) (func() ProtocolProcessor, bool) {
	processorRegistryMu.RLock()
	defer processorRegistryMu.RUnlock()
	factory, ok := processorRegistry[protocolID]
	return factory, ok
}

// GetAllProcessors returns a copy of all registered processor factories.
func GetAllProcessors() map[string]func() ProtocolProcessor {
	processorRegistryMu.RLock()
	defer processorRegistryMu.RUnlock()
	result := make(map[string]func() ProtocolProcessor, len(processorRegistry))
	for k, v := range processorRegistry {
		result[k] = v
	}
	return result
}

// resetProcessorRegistry replaces the registry map while holding the write lock.
// It is intended for use in tests only.
func resetProcessorRegistry(m map[string]func() ProtocolProcessor) {
	processorRegistryMu.Lock()
	defer processorRegistryMu.Unlock()
	processorRegistry = m
}
