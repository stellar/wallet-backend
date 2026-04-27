package services

import (
	"fmt"
	"sort"
	"sync"
)

// validatorRegistryMu guards concurrent access to validatorRegistry.
var validatorRegistryMu sync.RWMutex

// validatorRegistry holds factory functions keyed by protocol ID. Factories
// receive a ProtocolDeps so per-protocol packages can avoid package-level
// mutable state (no SetDependencies pattern required).
var validatorRegistry = map[string]func(ProtocolDeps) ProtocolValidator{}

// RegisterValidator registers a validator factory for a protocol ID. Called
// from per-protocol package init() functions.
func RegisterValidator(protocolID string, factory func(ProtocolDeps) ProtocolValidator) {
	validatorRegistryMu.Lock()
	defer validatorRegistryMu.Unlock()
	validatorRegistry[protocolID] = factory
}

// GetValidator returns the validator factory for a protocol ID, if registered.
func GetValidator(protocolID string) (func(ProtocolDeps) ProtocolValidator, bool) {
	validatorRegistryMu.RLock()
	defer validatorRegistryMu.RUnlock()
	factory, ok := validatorRegistry[protocolID]
	return factory, ok
}

// GetAllValidatorIDs returns the registered protocol IDs in sorted order so
// callers that iterate the full set get deterministic ordering.
func GetAllValidatorIDs() []string {
	validatorRegistryMu.RLock()
	defer validatorRegistryMu.RUnlock()
	ids := make([]string, 0, len(validatorRegistry))
	for id := range validatorRegistry {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// BuildValidators materializes validators for the given protocol IDs from the
// registry. Returns an error if any ID has no registered factory. Order
// matches the input slice; callers that need first-match-wins should pass IDs
// in protocol-id sort order or call GetAllValidatorIDs().
func BuildValidators(deps ProtocolDeps, protocolIDs []string) ([]ProtocolValidator, error) {
	out := make([]ProtocolValidator, 0, len(protocolIDs))
	for _, pid := range protocolIDs {
		factory, ok := GetValidator(pid)
		if !ok {
			return nil, fmt.Errorf("no validator registered for protocol %q", pid)
		}
		v := factory(deps)
		if v == nil {
			return nil, fmt.Errorf("validator factory for protocol %q returned nil", pid)
		}
		out = append(out, v)
	}
	return out, nil
}
