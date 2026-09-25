package processors

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// ContractDeployProcessor emits state changes for contract deployments.
type ContractDeployProcessor struct {
	networkPassphrase string
	metricsService    *metrics.IngestionMetrics
}

func NewContractDeployProcessor(networkPassphrase string, metricsService *metrics.IngestionMetrics) *ContractDeployProcessor {
	return &ContractDeployProcessor{
		networkPassphrase: networkPassphrase,
		metricsService:    metricsService,
	}
}

func (p *ContractDeployProcessor) Name() string {
	return "contract_deploy"
}

func (p *ContractDeployProcessor) StateChangeSubBase() int64 {
	return types.StateChangeSubBaseContractDeploy
}

// ProcessOperation emits a state change for each contract deployment (including subinvocations).
func (p *ContractDeployProcessor) ProcessOperation(_ context.Context, op *TransactionOperationWrapper) ([]types.StateChange, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("ContractDeployProcessor").Observe(duration)
		}
	}()

	if op.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return nil, ErrInvalidOpType
	}
	// A failed transaction deploys nothing, whatever its host function declares.
	if !op.Transaction.Successful() {
		return nil, nil
	}
	invokeHostOp := op.Operation.Body.MustInvokeHostFunctionOp()

	opID := op.ID()
	builder := NewStateChangeBuilder(op.Transaction.Ledger.LedgerSequence(), op.LedgerClosed.Unix(), op.TransactionID(), p.metricsService).
		WithOperationID(opID).
		WithCategory(types.StateChangeCategoryAccount).
		WithReason(types.StateChangeReasonCreate)

	var stateChanges []types.StateChange
	seen := map[string]struct{}{}

	emitCreate := func(contractID string, fromAddr xdr.ContractIdPreimageFromAddress) error {
		// The host rejects a deployer address it can't convert to a host object, so a deploy
		// from one never creates a contract and there is nothing to record.
		deployerAddr, storable, err := deployerAddressString(fromAddr.Address)
		if err != nil {
			return fmt.Errorf("deployer address to string: %w", err)
		}
		if !storable {
			return nil
		}
		if _, ok := seen[contractID]; ok {
			return nil
		}
		seen[contractID] = struct{}{}
		stateChanges = append(stateChanges, builder.Clone().WithAccount(contractID).WithCreator(deployerAddr).Build())
		return nil
	}

	// The top-level host function is authenticated: the host calls require_auth on the
	// FromAddress, so this (successful) operation proves the deploy and its deployer.
	hf := invokeHostOp.HostFunction
	switch hf.Type {
	case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
		if err := p.emitTopLevel(hf.MustCreateContract().ContractIdPreimage, emitCreate); err != nil {
			return nil, err
		}
	case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
		if err := p.emitTopLevel(hf.MustCreateContractV2().ContractIdPreimage, emitCreate); err != nil {
			return nil, err
		}
	case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm, xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
		// no-op
	}

	if len(invokeHostOp.Auth) == 0 {
		return stateChanges, nil
	}

	// Nested deploys are declared in the auth tree, which the submitter controls and the
	// host does not check for unmatched entries — a declared
	// CreateContract is recorded only when the meta holds the created instance entry,
	// which proves the deploy ran; the declaration then supplies the deployer address.
	created, err := createdContractInstances(op)
	if err != nil {
		return nil, err
	}
	var walkInvocation func(inv xdr.SorobanAuthorizedInvocation) error
	walkInvocation = func(inv xdr.SorobanAuthorizedInvocation) error {
		// Bind the args to a local first: a field of a function's return value is not
		// addressable.
		var preimage *xdr.ContractIdPreimage
		switch inv.Function.Type {
		case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractHostFn:
			cc := inv.Function.MustCreateContractHostFn()
			preimage = &cc.ContractIdPreimage
		case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractV2HostFn:
			cc := inv.Function.MustCreateContractV2HostFn()
			preimage = &cc.ContractIdPreimage
		case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn:
			// no-op
		}
		if preimage != nil && preimage.Type == xdr.ContractIdPreimageTypeContractIdPreimageFromAddress {
			fromAddr := preimage.MustFromAddress()
			contractID, err := calculateContractID(p.networkPassphrase, fromAddr)
			if err != nil {
				return fmt.Errorf("calculating contract ID: %w", err)
			}
			if _, ok := created[contractID]; ok {
				if err := emitCreate(contractID, fromAddr); err != nil {
					return err
				}
			}
		}
		for _, sub := range inv.SubInvocations {
			if err := walkInvocation(sub); err != nil {
				return err
			}
		}
		return nil
	}
	for _, auth := range invokeHostOp.Auth {
		if err := walkInvocation(auth.RootInvocation); err != nil {
			return nil, err
		}
	}

	return stateChanges, nil
}

// emitTopLevel records the operation's own CreateContract when its preimage names a
// deployer address. FromAsset preimages deploy a SAC, which has no creator account.
func (p *ContractDeployProcessor) emitTopLevel(preimage xdr.ContractIdPreimage, emit func(contractID string, fromAddr xdr.ContractIdPreimageFromAddress) error) error {
	if preimage.Type != xdr.ContractIdPreimageTypeContractIdPreimageFromAddress {
		return nil
	}
	fromAddr := preimage.MustFromAddress()
	contractID, err := calculateContractID(p.networkPassphrase, fromAddr)
	if err != nil {
		return fmt.Errorf("calculating contract ID: %w", err)
	}
	return emit(contractID, fromAddr)
}

// createdContractInstances returns the C-addresses whose contract instance entry this
// operation created, per the ledger entry changes in its meta.
func createdContractInstances(op *TransactionOperationWrapper) (map[string]struct{}, error) {
	changes, err := op.Transaction.GetOperationChanges(op.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}
	created := map[string]struct{}{}
	for _, change := range changes {
		// ChangeType, not Pre == nil: a protocol-23 hot-archive restore also surfaces
		// with no Pre, and a restored instance is not a deploy.
		if change.Type != xdr.LedgerEntryTypeContractData || change.ChangeType != xdr.LedgerEntryChangeTypeLedgerEntryCreated || change.Post == nil {
			continue
		}
		contractData := change.Post.Data.MustContractData()
		if contractData.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
			continue
		}
		contractID, err := contractData.Contract.String()
		if err != nil {
			return nil, fmt.Errorf("converting contract address to string: %w", err)
		}
		created[contractID] = struct{}{}
	}
	return created, nil
}
