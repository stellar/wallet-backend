package processors

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// ProtocolContractsProcessor extracts contract-to-WASM mappings from ContractData Instance entries.
// It processes ledger changes to identify contract deployments for protocol tracking.
type ProtocolContractsProcessor struct {
	metricsService *metrics.IngestionMetrics
}

// NewProtocolContractsProcessor creates a new protocol contract processor.
func NewProtocolContractsProcessor(metricsService *metrics.IngestionMetrics) *ProtocolContractsProcessor {
	return &ProtocolContractsProcessor{
		metricsService: metricsService,
	}
}

// Name returns the processor name for logging and metrics.
func (p *ProtocolContractsProcessor) Name() string {
	return "protocol_contracts"
}

// ProcessOperation extracts contract-to-WASM mappings from an operation's ledger changes.
// Only processes ContractData Instance entries with WASM executables.
func (p *ProtocolContractsProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]data.ProtocolContracts, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("ProtocolContractsProcessor").Observe(duration)
		}
	}()

	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var contracts []data.ProtocolContracts
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractData || change.Post == nil {
			continue
		}

		contractData := change.Post.Data.MustContractData()
		if contractData.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
			continue
		}

		contractIDBytes, ok := contractData.Contract.GetContractId()
		if !ok {
			continue
		}

		// Key.Type and Val.Type are independent XDR unions — a malformed
		// entry with a contract-instance Key but a non-instance Val must
		// not panic the ingest goroutine. Use GetInstance + ok rather than
		// MustInstance.
		instance, ok := contractData.Val.GetInstance()
		if !ok {
			continue
		}
		// Switched rather than compared so that a protocol adding a fourth
		// executable kind fails the exhaustive linter instead of being skipped.
		switch instance.Executable.Type {
		case xdr.ContractExecutableTypeContractExecutableWasm:
			if instance.Executable.WasmHash == nil {
				continue
			}

			hash := *instance.Executable.WasmHash

			contracts = append(contracts, data.ProtocolContracts{
				ContractID: types.HashBytea(hex.EncodeToString(contractIDBytes[:])),
				WasmHash:   types.HashBytea(hex.EncodeToString(hash[:])),
			})

		case xdr.ContractExecutableTypeContractExecutableStellarAsset:
			// SACs carry no WASM and are tracked by the SAC processors.

		case xdr.ContractExecutableTypeContractExecutableExternalRef:
			// CAP-0085: the executable is an (owner, tag) pair naming an entry in
			// another contract's storage, so there is no WASM hash to record and
			// the contract cannot be classified. Count and log it rather than
			// dropping it silently, so the gap is visible if one ever appears.
			if p.metricsService != nil {
				p.metricsService.ExternalRefContractsTotal.Inc()
			}
			log.Ctx(ctx).Warnf(
				"contract %s has an external-ref executable (%s); leaving it unclassified",
				hex.EncodeToString(contractIDBytes[:]),
				DescribeExternalRef(instance.Executable),
			)
		}
	}

	return contracts, nil
}

// DescribeExternalRef renders a CAP-0085 external reference for a log line.
// The owner address is best-effort: a reference whose owner will not encode is
// still worth reporting, so the raw type stands in rather than dropping the
// whole message.
func DescribeExternalRef(executable xdr.ContractExecutable) string {
	// The generated GetExternalRef dereferences the arm pointer once the
	// discriminant matches, so it is not a nil guard. Check the pointer.
	ref := executable.ExternalRef
	if ref == nil {
		return "external ref missing"
	}
	owner, err := ref.ExecutableOwner.String()
	if err != nil {
		owner = fmt.Sprintf("unencodable owner of type %s", ref.ExecutableOwner.Type)
	}
	return fmt.Sprintf("owner=%s tag=%q", owner, string(ref.Tag))
}
