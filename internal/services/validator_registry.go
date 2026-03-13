package services

// validatorRegistry holds factory functions keyed by protocol ID.
// Validators register themselves via init() using RegisterValidator.
var validatorRegistry = map[string]func() ProtocolValidator{}

// RegisterValidator registers a validator factory for a protocol ID.
// Called from init() in each validator file.
func RegisterValidator(protocolID string, factory func() ProtocolValidator) {
	validatorRegistry[protocolID] = factory
}

// GetValidator returns the validator factory for a protocol ID, if registered.
func GetValidator(protocolID string) (func() ProtocolValidator, bool) {
	factory, ok := validatorRegistry[protocolID]
	return factory, ok
}
