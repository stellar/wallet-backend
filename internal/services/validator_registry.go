package services

import (
	"fmt"
	"sort"
)

// validatorRegistry holds factory functions keyed by protocol ID. Factories
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
// ingestService.protocolValidators (which is built once at startup and never
// re-reads the registry).
var validatorRegistry = map[string]func(ProtocolDeps) ProtocolValidator{}

// RegisterValidator registers a validator factory for a protocol ID. Called
// from per-protocol package init() functions.
func RegisterValidator(protocolID string, factory func(ProtocolDeps) ProtocolValidator) {
	validatorRegistry[protocolID] = factory
}

// GetValidator returns the validator factory for a protocol ID, if registered.
func GetValidator(protocolID string) (func(ProtocolDeps) ProtocolValidator, bool) {
	factory, ok := validatorRegistry[protocolID]
	return factory, ok
}

// GetAllValidatorIDs returns the registered protocol IDs in sorted order so
// callers that iterate the full set get deterministic ordering.
func GetAllValidatorIDs() []string {
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
