package services

import "sync"

// registryMu guards concurrent access to validatorRegistry.
var registryMu sync.RWMutex

// validatorRegistry holds factory functions keyed by protocol ID.
var validatorRegistry = map[string]func() ProtocolValidator{}

// RegisterValidator registers a validator factory for a protocol ID.
func RegisterValidator(protocolID string, factory func() ProtocolValidator) {
	registryMu.Lock()
	defer registryMu.Unlock()
	validatorRegistry[protocolID] = factory
}

// GetValidator returns the validator factory for a protocol ID, if registered.
func GetValidator(protocolID string) (func() ProtocolValidator, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	factory, ok := validatorRegistry[protocolID]
	return factory, ok
}

// resetRegistry replaces the registry map while holding the write lock.
// It is intended for use in tests only.
func resetRegistry(m map[string]func() ProtocolValidator) {
	registryMu.Lock()
	defer registryMu.Unlock()
	validatorRegistry = m
}
