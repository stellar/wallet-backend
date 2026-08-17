package blend

import (
	"github.com/stellar/wallet-backend/internal/services"
)

// init wires Blend into the framework's registries. Both factories receive
// services.ProtocolDeps and pull the fields they need from there — there is
// no Blend-specific dependency struct exposed outside this package, and no
// cmd/* or internal/ingest/* code needs to know what Blend needs.
//
// To use Blend, callers blank-import this package (the import alone runs
// init) and then call services.BuildValidators / services.BuildProcessors
// with a single shared ProtocolDeps.
func init() {
	services.RegisterValidator(ProtocolID, func(deps services.ProtocolDeps) services.ProtocolValidator {
		return newValidator(deps)
	})
	services.RegisterProcessor(ProtocolID, func(deps services.ProtocolDeps) services.ProtocolProcessor {
		return newProcessor(deps)
	})
}
