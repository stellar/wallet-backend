// SACInstanceProcessor extracts SAC contract metadata from contract instance ledger entries.
// It processes instance entries to extract asset code and issuer for SAC tokens.
package processors

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// SACInstanceProcessor processes ledger changes to extract SAC contract instance metadata.
// SAC instances are stored in contract data entries with a defined format:
// Key: ScvLedgerKeyContractInstance
// Value: Contains the SAC asset information (code and issuer)
type SACInstanceProcessor struct {
	networkPassphrase string
}

// NewSACInstanceProcessor creates a new SAC instance processor.
func NewSACInstanceProcessor(networkPassphrase string) *SACInstanceProcessor {
	return &SACInstanceProcessor{
		networkPassphrase: networkPassphrase,
	}
}

// Name returns the processor name for logging and metrics.
func (p *SACInstanceProcessor) Name() string {
	return "sac_instances"
}

// ProcessOperation extracts SAC contract metadata from an operation's ledger changes.
// Returns Contract structs with asset code and issuer for database insertion.
// Only processes contract instance entries that represent SAC tokens.
func (p *SACInstanceProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]*data.Contract, error) {
	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var contracts []*data.Contract
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractData || change.Post == nil {
			continue
		}

		contractData := change.Post.Data.MustContractData()
		if contractData.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
			continue
		}

		asset, ok := sac.AssetFromContractData(*change.Post, p.networkPassphrase)
		if !ok {
			continue
		}

		contractIDBytes, ok := contractData.Contract.GetContractId()
		if !ok {
			continue
		}
		contractID := strkey.MustEncode(strkey.VersionByteContract, contractIDBytes[:])

		var assetType xdr.AssetType
		var code, issuer string
		asset.Extract(&assetType, &code, &issuer)

		name := fmt.Sprintf("%s:%s", code, issuer)
		contracts = append(contracts, &data.Contract{
			ID:         data.DeterministicContractID(contractID),
			ContractID: contractID,
			Type:       string(types.ContractTypeSAC),
			Code:       &code,
			Issuer:     &issuer,
			Name:       &name,
			Symbol:     &code,
			Decimals:   7,
		})
	}

	return contracts, nil
}
