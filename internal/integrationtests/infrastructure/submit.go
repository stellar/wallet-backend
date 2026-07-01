package infrastructure

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/utils"
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

	// Fee-bump fixture: its fee source moves only in the fee phase (issue #637).
	if err := submitFeeBumpFixture(ctx, env); err != nil {
		return fmt.Errorf("submitting fee-bump fixture: %w", err)
	}

	// Soroban refund fixture: its source pays a refunded resource fee and is touched by its own op.
	if err := submitSorobanRefundFixture(ctx, env); err != nil {
		return fmt.Errorf("submitting soroban refund fixture: %w", err)
	}

	return nil
}

// FeeBumpFixtureFeeStroops is the exact fee charged to the fee-bump fixture's fee source:
// MinBaseFee × (1 inner op + 1) = 200 stroops. Declaring the network minimum makes the
// charge deterministic, so the indexed balance can be asserted exactly.
const FeeBumpFixtureFeeStroops = txnbuild.MinBaseFee * 2

// submitFeeBumpFixture submits a fee-bump transaction whose fee source (env.FeeBumpSourceKP)
// is absent from every operation's meta — its native balance moves only in the fee phase.
// This exercises issue #637 end-to-end: AccountsProcessor.ProcessTransactionFees must fold
// the fee debit into the fee source's native balance even though no operation touches it.
func submitFeeBumpFixture(ctx context.Context, env *TestEnvironment) error {
	rpcService := env.RPCService

	// Inner transaction: a payment between two OTHER accounts, so the fee source appears
	// in no operation meta.
	seq, err := rpcService.GetAccountLedgerSequence(env.PrimaryAccountKP.Address())
	if err != nil {
		return fmt.Errorf("getting inner source sequence: %w", err)
	}
	innerSource := txnbuild.SimpleAccount{AccountID: env.PrimaryAccountKP.Address(), Sequence: seq}
	innerTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &innerSource,
		Operations: []txnbuild.Operation{&txnbuild.Payment{
			Destination: env.SecondaryAccountKP.Address(),
			Amount:      DefaultPaymentAmount,
			Asset:       txnbuild.NativeAsset{},
		}},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
		IncrementSequenceNum: true,
	})
	if err != nil {
		return fmt.Errorf("building inner transaction: %w", err)
	}
	innerTx, err = innerTx.Sign(networkPassphrase, env.PrimaryAccountKP)
	if err != nil {
		return fmt.Errorf("signing inner transaction: %w", err)
	}

	// Fee bump: env.FeeBumpSourceKP pays the fee and is otherwise untouched.
	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
		Inner:      innerTx,
		FeeAccount: env.FeeBumpSourceKP.Address(),
		BaseFee:    txnbuild.MinBaseFee,
	})
	if err != nil {
		return fmt.Errorf("building fee-bump transaction: %w", err)
	}
	feeBumpTx, err = feeBumpTx.Sign(networkPassphrase, env.FeeBumpSourceKP)
	if err != nil {
		return fmt.Errorf("signing fee-bump transaction: %w", err)
	}

	txXDR, err := feeBumpTx.Base64()
	if err != nil {
		return fmt.Errorf("encoding fee-bump transaction: %w", err)
	}

	res, err := rpcService.SendTransaction(txXDR)
	if err != nil {
		return fmt.Errorf("sending fee-bump transaction: %w", err)
	}
	if res.Status != entities.PendingStatus {
		return fmt.Errorf("fee-bump transaction %s failed with status %s, errorResultXdr=%+v", res.Hash, res.Status, res.ErrorResultXDR)
	}

	txResult, err := WaitForTransactionConfirmation(ctx, rpcService, res.Hash)
	if err != nil {
		return fmt.Errorf("waiting for fee-bump confirmation: %w", err)
	}
	if txResult.Status != entities.SuccessStatus {
		return fmt.Errorf("fee-bump transaction %s did not succeed: status=%s", res.Hash, txResult.Status)
	}

	log.Ctx(ctx).Infof("✅ Submitted fee-bump fixture; fee source %s charged %d stroops", env.FeeBumpSourceKP.Address(), FeeBumpFixtureFeeStroops)
	return nil
}

// SorobanRefundSurplusStroops is the resource fee the Soroban refund fixture declares above what
// simulation requires. Core refunds the unspent resource fee after apply, so declaring a surplus
// guarantees a post-apply refund large enough to make the balance assertion discriminating: if the
// refund were dropped, the indexed balance would fall short of the on-chain balance by this amount.
const SorobanRefundSurplusStroops = 1_000_000

// submitSorobanRefundFixture submits a Soroban transaction whose source (env.SorobanRefundSourceKP)
// over-declares its resource fee — so Core issues a post-apply refund — and is also the `from` of
// the native-XLM SAC transfer it invokes. The source's native balance therefore moves in all three
// phases of one ledger: the fee debit, the transfer operation, and the post-apply refund. This lets
// TestLiveIngestion_SorobanRefundSource_NativeBalanceMatchesChain assert the indexed balance equals
// the on-chain balance — proving the refund is folded in and reflects the operation applied before
// it (the crux of ranking phaseRefund above phaseOperation; see accounts.go accountSortKey).
func submitSorobanRefundFixture(ctx context.Context, env *TestEnvironment) error {
	rpcService := env.RPCService
	source := env.SorobanRefundSourceKP

	// Native-XLM SAC transfer: from = source (so the operation moves its balance) → PrimaryAccountKP.
	nativeContractIDBytes, err := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(networkPassphrase)
	if err != nil {
		return fmt.Errorf("getting native asset contract ID: %w", err)
	}
	nativeContractID := xdr.ContractId(nativeContractIDBytes)
	fromSCAddress, err := SCAccountID(source.Address())
	if err != nil {
		return fmt.Errorf("building from address: %w", err)
	}
	toSCAddress, err := SCAccountID(env.PrimaryAccountKP.Address())
	if err != nil {
		return fmt.Errorf("building to address: %w", err)
	}
	transferOp := &txnbuild.InvokeHostFunction{
		SourceAccount: source.Address(),
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &nativeContractID},
				FunctionName:    "transfer",
				Args: xdr.ScVec{
					{Type: xdr.ScValTypeScvAddress, Address: &fromSCAddress},
					{Type: xdr.ScValTypeScvAddress, Address: &toSCAddress},
					{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Hi: 0, Lo: xdr.Uint64(uint64(amount.MustParse(DefaultPaymentAmount)))}},
				},
			},
		},
	}

	seq, err := rpcService.GetAccountLedgerSequence(source.Address())
	if err != nil {
		return fmt.Errorf("getting source sequence: %w", err)
	}
	sourceAccount := txnbuild.SimpleAccount{AccountID: source.Address(), Sequence: seq}

	// Simulate to obtain the resource footprint (SorobanData) and the minimum resource fee.
	simTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &sourceAccount,
		Operations:           []txnbuild.Operation{transferOp},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
		IncrementSequenceNum: false,
	})
	if err != nil {
		return fmt.Errorf("building simulation transaction: %w", err)
	}
	simXDR, err := simTx.Base64()
	if err != nil {
		return fmt.Errorf("encoding simulation transaction: %w", err)
	}
	sim, err := rpcService.SimulateTransaction(simXDR, entities.RPCResourceConfig{})
	if err != nil {
		return fmt.Errorf("simulating transaction: %w", err)
	}
	if sim.Error != "" {
		return fmt.Errorf("simulation failed: %s", sim.Error)
	}

	// from == source, so require_auth is satisfied by the transaction signature; apply whatever
	// (source-account) auth entries simulation returned.
	if len(sim.Results) > 0 {
		transferOp.Auth = sim.Results[0].Auth
	}

	minResourceFee, err := strconv.ParseInt(sim.MinResourceFee, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing MinResourceFee: %w", err)
	}

	// Over-declare the resource fee; the surplus is refunded after apply.
	sorobanData := sim.TransactionData
	sorobanData.ResourceFee += SorobanRefundSurplusStroops
	transferOp.Ext = xdr.TransactionExt{V: 1, SorobanData: &sorobanData}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &sourceAccount,
		Operations:           []txnbuild.Operation{transferOp},
		BaseFee:              minResourceFee + SorobanRefundSurplusStroops + txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
		IncrementSequenceNum: true,
	})
	if err != nil {
		return fmt.Errorf("building transaction: %w", err)
	}
	tx, err = tx.Sign(networkPassphrase, source)
	if err != nil {
		return fmt.Errorf("signing transaction: %w", err)
	}
	txXDR, err := tx.Base64()
	if err != nil {
		return fmt.Errorf("encoding transaction: %w", err)
	}

	res, err := rpcService.SendTransaction(txXDR)
	if err != nil {
		return fmt.Errorf("sending transaction: %w", err)
	}
	if res.Status != entities.PendingStatus {
		return fmt.Errorf("soroban refund transaction %s failed with status %s, errorResultXdr=%+v", res.Hash, res.Status, res.ErrorResultXDR)
	}

	txResult, err := WaitForTransactionConfirmation(ctx, rpcService, res.Hash)
	if err != nil {
		return fmt.Errorf("waiting for soroban refund confirmation: %w", err)
	}
	if txResult.Status != entities.SuccessStatus {
		return fmt.Errorf("soroban refund transaction %s did not succeed: status=%s", res.Hash, txResult.Status)
	}

	log.Ctx(ctx).Infof("✅ Submitted Soroban refund fixture; source %s over-declared %d stroops", source.Address(), SorobanRefundSurplusStroops)
	return nil
}

// GetOnChainNativeBalance reads an account's current native (XLM) balance in stroops directly from
// RPC, giving tests a ground-truth value to assert the indexed balance against.
func GetOnChainNativeBalance(rpcService services.RPCService, address string) (int64, error) {
	keyXDR, err := utils.GetAccountLedgerKey(address)
	if err != nil {
		return 0, fmt.Errorf("building account ledger key: %w", err)
	}
	result, err := rpcService.GetLedgerEntries([]string{keyXDR})
	if err != nil {
		return 0, fmt.Errorf("getting ledger entries: %w", err)
	}
	if len(result.Entries) == 0 {
		return 0, fmt.Errorf("no ledger entry found for account %s", address)
	}
	var data xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(result.Entries[0].DataXDR, &data); err != nil {
		return 0, fmt.Errorf("decoding account ledger entry: %w", err)
	}
	account, ok := data.GetAccount()
	if !ok {
		return 0, fmt.Errorf("ledger entry for %s is not an account", address)
	}
	return int64(account.Balance), nil
}

// buildSignAndSubmit rebuilds a use case's transaction with a real funded source account,
// signs it, and submits to RPC.
func buildSignAndSubmit(
	ctx context.Context,
	uc *UseCase,
	rpcService services.RPCService,
	sourceKP *keypair.Full,
) error {
	if uc.DelayTime > 0 {
		log.Ctx(ctx).Infof("%s delaying for %s", uc.Name(), uc.DelayTime)
		time.Sleep(uc.DelayTime)
	}

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
