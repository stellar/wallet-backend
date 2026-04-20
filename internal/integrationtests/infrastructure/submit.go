package infrastructure

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
)

// SubmitUseCases builds, signs, and submits all use case transactions directly to RPC,
// then waits for confirmations and populates each UseCase's result fields.
// It also handles the second-pass claim/clawback use cases that depend on
// claimable balance IDs extracted from the first pass.
func SubmitUseCases(ctx context.Context, env *TestEnvironment) error {
	rpcService := env.RPCService
	sourceKP := env.TxSourceAccountKP

	// Wait for RPC health
	log.Ctx(ctx).Info("Waiting for RPC to become healthy before submitting transactions...")
	if err := WaitForRPCHealthAndRun(ctx, rpcService, 40*time.Second); err != nil {
		return fmt.Errorf("RPC service did not become healthy: %w", err)
	}

	// Submit all regular use case transactions sequentially
	log.Ctx(ctx).Info("Submitting use case transactions...")
	for _, uc := range env.UseCases {
		if err := buildSignAndSubmit(ctx, uc, rpcService, sourceKP); err != nil {
			return fmt.Errorf("submitting %s: %w", uc.Name(), err)
		}
	}

	// Wait for confirmations
	log.Ctx(ctx).Info("Waiting for transaction confirmations...")
	for _, uc := range env.UseCases {
		txResult, err := WaitForTransactionConfirmation(ctx, rpcService, uc.SendTransactionResult.Hash)
		if err != nil {
			return fmt.Errorf("waiting for confirmation of %s: %w", uc.Name(), err)
		}
		uc.GetTransactionResult = txResult
		log.Ctx(ctx).Info(RenderResult(uc))
	}

	// Extract claimable balance IDs
	createCBUseCase := FindUseCase(env.UseCases, "Stellarclassic/createClaimableBalanceOps")
	if createCBUseCase == nil {
		return fmt.Errorf("createClaimableBalanceOps use case not found")
	}
	balanceIDs, err := ExtractClaimableBalanceIDsFromMeta(createCBUseCase.GetTransactionResult.ResultMetaXDR)
	if err != nil {
		return fmt.Errorf("extracting claimable balance IDs: %w", err)
	}
	if len(balanceIDs) != 2 {
		return fmt.Errorf("expected 2 claimable balance IDs, got %d", len(balanceIDs))
	}
	env.ClaimBalanceID = balanceIDs[0]
	env.ClawbackBalanceID = balanceIDs[1]
	log.Ctx(ctx).Infof("Extracted balance IDs: [0]=%s, [1]=%s", balanceIDs[0], balanceIDs[1])

	// Prepare and submit claim/clawback use cases
	fixtures := &Fixtures{
		NetworkPassphrase:     env.NetworkPassphrase,
		PrimaryAccountKP:      env.PrimaryAccountKP,
		SecondaryAccountKP:    env.SecondaryAccountKP,
		SponsoredNewAccountKP: env.SponsoredNewAccountKP,
		RPCService:            env.RPCService,
	}
	claimAndClawbackUseCases, err := fixtures.PrepareClaimAndClawbackUseCases(balanceIDs[0], balanceIDs[1])
	if err != nil {
		return fmt.Errorf("preparing claim/clawback use cases: %w", err)
	}
	env.ClaimAndClawbackUseCases = claimAndClawbackUseCases

	log.Ctx(ctx).Info("Submitting claim/clawback transactions...")
	for _, uc := range claimAndClawbackUseCases {
		if err := buildSignAndSubmit(ctx, uc, rpcService, sourceKP); err != nil {
			return fmt.Errorf("submitting %s: %w", uc.Name(), err)
		}
	}

	for _, uc := range claimAndClawbackUseCases {
		txResult, err := WaitForTransactionConfirmation(ctx, rpcService, uc.SendTransactionResult.Hash)
		if err != nil {
			return fmt.Errorf("waiting for confirmation of %s: %w", uc.Name(), err)
		}
		uc.GetTransactionResult = txResult
		if txResult.Status == entities.FailedStatus {
			return fmt.Errorf("transaction for %s failed", uc.Name())
		}
		log.Ctx(ctx).Info(RenderResult(uc))
	}

	return nil
}

// buildSignAndSubmit rebuilds a use case's transaction with a real funded source account,
// signs it, and submits to RPC.
func buildSignAndSubmit(
	ctx context.Context,
	uc *UseCase,
	rpcService services.RPCService,
	sourceKP *keypair.Full,
) error {
	// Parse the requested transaction to extract operations
	requestedTx, err := parseTxXDR(uc.RequestedTransaction.TransactionXdr)
	if err != nil {
		return fmt.Errorf("parsing requested transaction: %w", err)
	}

	// Get current sequence number from RPC
	seq, err := rpcService.GetAccountLedgerSequence(sourceKP.Address())
	if err != nil {
		return fmt.Errorf("getting sequence number: %w", err)
	}

	sourceAccount := txnbuild.SimpleAccount{
		AccountID: sourceKP.Address(),
		Sequence:  seq,
	}

	// Rebuild transaction with real source account
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &sourceAccount,
		Operations:           requestedTx.Operations(),
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
		IncrementSequenceNum: true,
	})
	if err != nil {
		return fmt.Errorf("building transaction: %w", err)
	}

	// Sign with source account + use case signers (deduplicated to avoid txBAD_AUTH_EXTRA)
	signers := []*keypair.Full{sourceKP}
	for _, s := range uc.TxSigners.Slice() {
		if s.Address() != sourceKP.Address() {
			signers = append(signers, s)
		}
	}
	signedTx, err := tx.Sign(networkPassphrase, signers...)
	if err != nil {
		return fmt.Errorf("signing transaction: %w", err)
	}

	txXDR, err := signedTx.Base64()
	if err != nil {
		return fmt.Errorf("encoding signed transaction: %w", err)
	}

	// Submit to RPC
	res, err := rpcService.SendTransaction(txXDR)
	if err != nil {
		return fmt.Errorf("sending transaction: %w", err)
	}
	if res.Status != entities.PendingStatus {
		return fmt.Errorf("transaction %s failed with status %s, errorResultXdr=%+v",
			res.Hash, res.Status, res.ErrorResultXDR)
	}

	uc.SendTransactionResult = res
	log.Ctx(ctx).Debugf("[%s] submitted: hash=%s", uc.Name(), res.Hash)
	return nil
}

// parseTxXDR parses a base64-encoded transaction XDR into a txnbuild.Transaction.
func parseTxXDR(txXDR string) (*txnbuild.Transaction, error) {
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return nil, fmt.Errorf("building transaction from XDR: %w", err)
	}
	tx, ok := genericTx.Transaction()
	if !ok {
		return nil, fmt.Errorf("expected a regular transaction, not a fee bump")
	}
	return tx, nil
}
