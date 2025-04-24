package integrationtests

// import (
// 	"context"
// 	"fmt"

// 	"github.com/stellar/go/keypair"
// 	"github.com/stellar/go/txnbuild"
// 	"github.com/stellar/go/xdr"

// 	"github.com/stellar/wallet-backend/internal/services"
// )

// type SimulationResult struct {
// 	Tx                 *txnbuild.Transaction
// 	SimulationResponse string
// }

// // SimulateContract simulates a Soroban contract method.
// func SimulateContract(
// 	ctx context.Context,
// 	rpcService *services.RPCService,
// 	contractID string,
// 	method string,
// 	args []xdr.ScVal,
// 	kps []keypair.KP,
// ) (*SimulationResult, error) {
// 	// 1) Fetch source account
// 	accountReq := soroban.AccountRequest{AccountID: s.sourceKP.Address()}
// 	sourceAccount, err := s.client.GetAccount(ctx, accountReq)
// 	if err != nil {
// 		return nil, fmt.Errorf("%s: %w", helpers.ErrFetchAccount, err)
// 	}

// 	// 2) Build the host function operation
// 	contract := soroban.NewContract(contractID)
// 	op := contract.Call(method, args...)

// 	txParams := txnbuild.TransactionParams{
// 		SourceAccount:        &sourceAccount,
// 		Operations:           []txnbuild.Operation{op},
// 		BaseFee:              txnbuild.MustParseAmount(s.fee),
// 		Timebounds:           txnbuild.NewTimeout(uint64(s.timeout.Seconds())),
// 		Network:              network.Passphrase(s.networkPassphrase),
// 		IncrementSequenceNum: true,
// 	}
// 	tx, err := txnbuild.NewTransaction(txParams)
// 	if err != nil {
// 		return nil, fmt.Errorf("building transaction: %w", err)
// 	}

// 	// 3) First simulation
// 	simResp, err := s.client.SimulateTransaction(ctx, tx)
// 	if err != nil {
// 		return nil, fmt.Errorf("%s (simulation 1): %w", helpers.ErrTxSimFailed, err)
// 	}
// 	if !simResp.Successful() {
// 		return nil, fmt.Errorf("%s (simulation 1): %+v", helpers.ErrTxSimFailed, simResp)
// 	}

// 	// 4) If there are auth entries to sign, sign and re-simulate
// 	if len(signers) > 0 {
// 		authEntries := simResp.Result.Auth

// 		tx, err = s.signAuthEntries(ctx, authEntries, tx, contractID, signers)
// 		if err != nil {
// 			return nil, err
// 		}

// 		simResp, err = s.client.SimulateTransaction(ctx, tx)
// 		if err != nil {
// 			return nil, fmt.Errorf("%s (simulation 2): %w", helpers.ErrTxSimFailed, err)
// 		}
// 		if !simResp.Successful() {
// 			return nil, fmt.Errorf("%s (simulation 2): %+v", helpers.ErrTxSimFailed, simResp)
// 		}
// 	}

// 	// 5) Return the built tx and the final simulation response
// 	return &types.SimulationResult{
// 		Tx:                 tx,
// 		SimulationResponse: simResp,
// 	}, nil
// }
