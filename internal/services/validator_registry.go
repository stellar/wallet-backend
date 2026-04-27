package services

import "sync"

// registryMu guards concurrent access to validatorRegistry.
var registryMu sync.RWMutex

// validatorRegistry holds validator instances keyed by protocol ID.
var validatorRegistry = map[string]ProtocolValidator{}

// RegisterValidator registers a validator instance for a protocol ID.
func RegisterValidator(protocolID string, v ProtocolValidator) {
	registryMu.Lock()
	defer registryMu.Unlock()
	validatorRegistry[protocolID] = v
}

// GetValidator returns the validator for a protocol ID, if registered.
func GetValidator(protocolID string) (ProtocolValidator, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	v, ok := validatorRegistry[protocolID]
	return v, ok
}

// GetAllValidators returns a snapshot of every registered validator.
// Order is unspecified — callers that depend on a deterministic match order
// must sort the result by ProtocolID() themselves.
func GetAllValidators() []ProtocolValidator {
	registryMu.RLock()
	defer registryMu.RUnlock()
	result := make([]ProtocolValidator, 0, len(validatorRegistry))
	for _, v := range validatorRegistry {
		result = append(result, v)
	}
	return result
}

// resetRegistry replaces the registry map while holding the write lock.
// It is intended for use in tests only.
func resetRegistry(m map[string]ProtocolValidator) {
	registryMu.Lock()
	defer registryMu.Unlock()
	validatorRegistry = m
}
