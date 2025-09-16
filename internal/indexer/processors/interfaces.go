// Interfaces for dependency injection in indexer processors.
// Contains minimal interfaces to avoid import cycles while maintaining testability.
package processors

import "github.com/stellar/wallet-backend/internal/entities"

// LedgerEntryProvider provides access to ledger entries from the Stellar network.
// This interface allows the EffectsProcessor to retrieve ledger data without creating
// an import cycle with the services package.
type LedgerEntryProvider interface {
	GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error)
}
