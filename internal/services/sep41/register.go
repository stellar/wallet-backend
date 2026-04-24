package sep41

import (
	"sync"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/services"
)

// Dependencies holds the runtime objects the SEP-41 processor needs. Populated by the cmd
// layer via SetDependencies after Models are built; the registered processor factory captures
// this package-level var by closure.
type Dependencies struct {
	NetworkPassphrase string
	Balances          sep41data.BalanceModelInterface
	Allowances        sep41data.AllowanceModelInterface
	ContractTokens    data.ContractModelInterface
	StateChanges      data.StateChangeWriter
}

var (
	depsMu sync.RWMutex
	deps   Dependencies
)

// SetDependencies stores the processor dependencies. Call this after building data.Models
// and before invoking services.GetAllProcessors / GetProcessor.
func SetDependencies(d Dependencies) {
	depsMu.Lock()
	defer depsMu.Unlock()
	deps = d
}

// currentDeps returns a snapshot of the dependencies.
func currentDeps() Dependencies {
	depsMu.RLock()
	defer depsMu.RUnlock()
	return deps
}

func init() {
	services.RegisterValidator(ProtocolID, NewValidator())
	services.RegisterProcessor(ProtocolID, func() services.ProtocolProcessor {
		return newProcessor(currentDeps())
	})
}
