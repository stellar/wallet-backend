package processors

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// ContractDeployProcessor emits state changes for contract deployments.
type ContractDeployProcessor struct {
	networkPassphrase string
	metricsService    MetricsServiceInterface
}

func NewContractDeployProcessor(networkPassphrase string, metricsService MetricsServiceInterface) *ContractDeployProcessor {
	return &ContractDeployProcessor{
		networkPassphrase: networkPassphrase,
		metricsService:    metricsService,
	}
}

func (p *ContractDeployProcessor) Name() string {
	return "contract_deploy"
}

// ProcessOperation emits a state change for each contract deployment (including subinvocations).
func (p *ContractDeployProcessor) ProcessOperation(_ context.Context, op *TransactionOperationWrapper) ([]types.StateChange, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.ObserveStateChangeProcessingDuration("ContractDeployProcessor", duration)
		}
	}()

	if op.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return nil, ErrInvalidOpType
	}
	invokeHostOp := op.Operation.Body.MustInvokeHostFunctionOp()

	opID := op.ID()
	builder := NewStateChangeBuilder(op.Transaction.Ledger.LedgerSequence(), op.LedgerClosed.Unix(), op.Transaction.Hash.HexString(), op.TransactionID(), p.metricsService).
		WithOperationID(opID).
		WithCategory(types.StateChangeCategoryAccount).
		WithReason(types.StateChangeReasonCreate)

	deployedContractsMap := map[string]types.StateChange{}

	processCreate := func(fromAddr xdr.ContractIdPreimageFromAddress) error {
		contractID, err := calculateContractID(p.networkPassphrase, fromAddr)
		if err != nil {
			return fmt.Errorf("calculating contract ID: %w", err)
		}
		deployerAddr, err := fromAddr.Address.String()
		if err != nil {
			return fmt.Errorf("deployer address to string: %w", err)
		}

		deployedContractsMap[contractID] = builder.Clone().WithAccount(contractID).WithDeployer(deployerAddr).Build()
		return nil
	}

	var walkInvocation func(inv xdr.SorobanAuthorizedInvocation) error
	walkInvocation = func(inv xdr.SorobanAuthorizedInvocation) error {
		switch inv.Function.Type {
		case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractHostFn:
			cc := inv.Function.MustCreateContractHostFn()
			if cc.ContractIdPreimage.Type == xdr.ContractIdPreimageTypeContractIdPreimageFromAddress {
				if err := processCreate(cc.ContractIdPreimage.MustFromAddress()); err != nil {
					return err
				}
			}
		case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractV2HostFn:
			cc := inv.Function.MustCreateContractV2HostFn()
			if cc.ContractIdPreimage.Type == xdr.ContractIdPreimageTypeContractIdPreimageFromAddress {
				if err := processCreate(cc.ContractIdPreimage.MustFromAddress()); err != nil {
					return err
				}
			}
		case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn:
			// no-op
		}
		for _, sub := range inv.SubInvocations {
			if err := walkInvocation(sub); err != nil {
				return err
			}
		}
		return nil
	}

	hf := invokeHostOp.HostFunction
	switch hf.Type {
	case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
		cc := hf.MustCreateContract()
		if cc.ContractIdPreimage.Type == xdr.ContractIdPreimageTypeContractIdPreimageFromAddress {
			if err := processCreate(cc.ContractIdPreimage.MustFromAddress()); err != nil {
				return nil, err
			}
		}
	case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
		cc := hf.MustCreateContractV2()
		if cc.ContractIdPreimage.Type == xdr.ContractIdPreimageTypeContractIdPreimageFromAddress {
			if err := processCreate(cc.ContractIdPreimage.MustFromAddress()); err != nil {
				return nil, err
			}
		}
	case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm, xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
		// no-op
	}

	for _, auth := range invokeHostOp.Auth {
		if err := walkInvocation(auth.RootInvocation); err != nil {
			return nil, err
		}
	}

	stateChanges := make([]types.StateChange, 0, len(deployedContractsMap))
	for _, sc := range deployedContractsMap {
		stateChanges = append(stateChanges, sc)
	}
	return stateChanges, nil
}
