// Package loadtest provides synthetic ledger generation for load testing ingestion.
// This file contains the main ledger generation logic that mirrors Horizon's approach.
package loadtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go/amount"
	goloadtest "github.com/stellar/go/ingest/loadtest"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

const (
	// DefaultTransactionsPerLedger is the default number of transactions per generated ledger
	DefaultTransactionsPerLedger = 100
	// DefaultTransfersPerTx is the default number of transfers per bulk transaction
	DefaultTransfersPerTx = 10
	// DefaultLedgerCount is the default number of ledgers to generate
	DefaultLedgerCount = 2
	// TransactionPollInterval is the interval between transaction status checks
	TransactionPollInterval = 500 * time.Millisecond
	// DefaultTransactionTimeout is the timeout for transaction building
	DefaultTransactionTimeout = 300
	// WorkerBatchSize is the number of transactions per worker
	WorkerBatchSize = 100
	// MaxAccountsPerTransaction is the max accounts to create in one tx
	MaxAccountsPerTransaction = 100
	// DefaultFundingAmount is the amount to fund each account with
	DefaultFundingAmount = "10000000"
)

// GeneratorConfig configures the synthetic ledger generator.
type GeneratorConfig struct {
	// TransactionsPerLedger is the number of transactions per generated ledger
	TransactionsPerLedger int
	// TransfersPerTx is the number of transfers per bulk transaction
	TransfersPerTx int
	// LedgerCount is the number of ledgers to generate
	LedgerCount int
	// OutputPath is the path to write the generated ledgers
	OutputPath string
}

// sorobanTransaction holds a prepared bulk transfer transaction
type sorobanTransaction struct {
	op             *txnbuild.InvokeHostFunction
	signer         *keypair.Full
	sequenceNumber int64
}

// Generate creates synthetic ledgers with bulk Soroban transfers.
// This mirrors Horizon's generate_ledgers_test.go approach.
func Generate(ctx context.Context, cfg GeneratorConfig) error {
	// Apply defaults
	if cfg.TransactionsPerLedger == 0 {
		cfg.TransactionsPerLedger = DefaultTransactionsPerLedger
	}
	if cfg.TransfersPerTx == 0 {
		cfg.TransfersPerTx = DefaultTransfersPerTx
	}
	if cfg.LedgerCount == 0 {
		cfg.LedgerCount = DefaultLedgerCount
	}
	if cfg.OutputPath == "" {
		return fmt.Errorf("output path is required")
	}

	// Validate configuration
	if cfg.TransactionsPerLedger%MaxAccountsPerTransaction != 0 {
		return fmt.Errorf("transactionsPerLedger (%d) must be a multiple of %d",
			cfg.TransactionsPerLedger, MaxAccountsPerTransaction)
	}
	if cfg.TransactionsPerLedger%WorkerBatchSize != 0 {
		return fmt.Errorf("transactionsPerLedger (%d) must be a multiple of %d",
			cfg.TransactionsPerLedger, WorkerBatchSize)
	}

	log.Infof("Starting ledger generation with config: txPerLedger=%d, transfersPerTx=%d, ledgerCount=%d",
		cfg.TransactionsPerLedger, cfg.TransfersPerTx, cfg.LedgerCount)

	// Start containers
	containers, err := StartLoadTestContainers(ctx)
	if err != nil {
		return fmt.Errorf("starting containers: %w", err)
	}
	defer func() { _ = containers.Close() }() //nolint:errcheck

	rpcURL, err := containers.RPCContainer.GetConnectionString(ctx)
	if err != nil {
		return fmt.Errorf("getting RPC URL: %w", err)
	}

	// Step 1: Deploy native asset SAC
	log.Info("Deploying native asset SAC...")
	if sacErr := deployNativeAssetSAC(containers, rpcURL); sacErr != nil {
		return fmt.Errorf("deploying native asset SAC: %w", sacErr)
	}

	// Step 2: Deploy soroban_bulk contract
	log.Info("Deploying soroban_bulk contract...")
	bulkContractID, err := deployBulkContract(containers, rpcURL)
	if err != nil {
		return fmt.Errorf("deploying bulk contract: %w", err)
	}
	log.Infof("Bulk contract deployed: %x", bulkContractID[:8])

	// Step 3: Create accounts (2x transactionsPerLedger - half senders, half recipients)
	log.Infof("Creating %d accounts...", 2*cfg.TransactionsPerLedger)
	signers, accounts, accountLedgers, err := createAccounts(containers, rpcURL, 2*cfg.TransactionsPerLedger)
	if err != nil {
		return fmt.Errorf("creating accounts: %w", err)
	}

	// Split into senders and recipients
	recipients := signers[cfg.TransactionsPerLedger:]
	signers = signers[:cfg.TransactionsPerLedger]
	accounts = accounts[:cfg.TransactionsPerLedger]

	// Step 4: Prepare bulk transfer transactions
	log.Info("Preparing bulk transfer transactions...")
	transactions, err := prepareBulkTransfers(containers, rpcURL, bulkContractID,
		signers, accounts, recipients, cfg.TransfersPerTx)
	if err != nil {
		return fmt.Errorf("preparing bulk transfers: %w", err)
	}

	// Step 5: Wait for one empty ledger before generating load
	log.Info("Waiting for empty ledger...")
	if waitErr := waitForEmptyLedger(ctx, containers, rpcURL, accountLedgers); waitErr != nil {
		return fmt.Errorf("waiting for empty ledger: %w", waitErr)
	}

	// Step 6: Submit transactions in parallel workers
	log.Infof("Submitting %d transactions across %d ledgers...", len(transactions), cfg.LedgerCount)
	ledgerMap := submitTransactionsInBatches(ctx, containers, rpcURL, transactions, cfg.LedgerCount)

	// Step 7: Determine ledger range
	startI32, endI32 := findLedgerRange(ledgerMap)
	if startI32 <= 2 {
		return fmt.Errorf("expected empty ledger preceding load: start=%d", startI32)
	}
	startI32-- // Include the empty ledger before
	start := uint32(startI32)
	end := uint32(endI32)
	log.Infof("Generated ledgers in range [%d, %d]", start, end)

	// Step 8: Wait for ledgers to be available in RPC
	log.Infof("Waiting for ledger %d to be available...", end)
	if ledgerErr := waitForLedger(ctx, containers, rpcURL, end); ledgerErr != nil {
		return fmt.Errorf("waiting for ledger: %w", ledgerErr)
	}

	// Step 9: Fetch and merge ledgers, write to output
	log.Infof("Fetching and merging ledgers to %s...", cfg.OutputPath)
	accountEntries, err := getAccountEntries(ctx, containers, rpcURL, accountLedgers, 2*cfg.TransactionsPerLedger)
	if err != nil {
		return fmt.Errorf("getting account entries: %w", err)
	}

	if err := mergeLedgers(containers, rpcURL, accountEntries, cfg.OutputPath,
		start, end, cfg.TransactionsPerLedger); err != nil {
		return fmt.Errorf("merging ledgers: %w", err)
	}

	log.Infof("Successfully generated ledgers to %s", cfg.OutputPath)
	return nil
}

// deployNativeAssetSAC deploys the Stellar Asset Contract for native XLM.
func deployNativeAssetSAC(containers *LoadTestContainers, rpcURL string) error {
	nativeAsset := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}
	deployOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
			CreateContract: &xdr.CreateContractArgs{
				ContractIdPreimage: xdr.ContractIdPreimage{
					Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
					FromAsset: &nativeAsset,
				},
				Executable: xdr.ContractExecutable{
					Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
				},
			},
		},
		SourceAccount: containers.MasterKeyPair.Address(),
	}

	_, err := infrastructure.ExecuteSorobanOperation(containers.HTTPClient, rpcURL, containers.MasterAccount, containers.MasterKeyPair, deployOp, false, 20)
	if err != nil {
		return fmt.Errorf("deploying native asset SAC: %w", err)
	}
	return nil
}

// deployBulkContract uploads and deploys the soroban_bulk.wasm contract.
func deployBulkContract(containers *LoadTestContainers, rpcURL string) (xdr.ContractId, error) {
	// Read WASM file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return xdr.ContractId{}, fmt.Errorf("failed to get caller information")
	}
	dir := filepath.Dir(filename)
	wasmPath := filepath.Join(dir, "../integrationtests/infrastructure/testdata/soroban_bulk.wasm")

	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		return xdr.ContractId{}, fmt.Errorf("reading WASM file: %w", err)
	}

	// Upload WASM
	uploadOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
			Wasm: &wasmBytes,
		},
		SourceAccount: containers.MasterKeyPair.Address(),
	}

	if _, uploadErr := infrastructure.ExecuteSorobanOperation(containers.HTTPClient, rpcURL, containers.MasterAccount, containers.MasterKeyPair, uploadOp, false, 20); uploadErr != nil {
		return xdr.ContractId{}, fmt.Errorf("uploading WASM: %w", uploadErr)
	}

	wasmHash := xdr.Hash(sha256.Sum256(wasmBytes))

	// Deploy contract
	salt := make([]byte, 32)
	if _, randErr := rand.Read(salt); randErr != nil {
		return xdr.ContractId{}, fmt.Errorf("generating salt: %w", randErr)
	}
	var saltHash xdr.Uint256
	copy(saltHash[:], salt)

	deployerAccountID := xdr.MustAddress(containers.MasterKeyPair.Address())
	preimage := xdr.ContractIdPreimage{
		Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		FromAddress: &xdr.ContractIdPreimageFromAddress{
			Address: xdr.ScAddress{
				Type:      xdr.ScAddressTypeScAddressTypeAccount,
				AccountId: &deployerAccountID,
			},
			Salt: saltHash,
		},
	}

	deployOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContractV2,
			CreateContractV2: &xdr.CreateContractArgsV2{
				ContractIdPreimage: preimage,
				Executable: xdr.ContractExecutable{
					Type:     xdr.ContractExecutableTypeContractExecutableWasm,
					WasmHash: &wasmHash,
				},
				ConstructorArgs: nil,
			},
		},
		SourceAccount: containers.MasterKeyPair.Address(),
	}

	// requireAuth=true for CreateContractV2 with constructor
	if _, deployErr := infrastructure.ExecuteSorobanOperation(containers.HTTPClient, rpcURL, containers.MasterAccount, containers.MasterKeyPair, deployOp, true, 20); deployErr != nil {
		return xdr.ContractId{}, fmt.Errorf("deploying contract: %w", deployErr)
	}

	// Calculate contract ID
	contractID, err := calculateContractID(infrastructure.NetworkPassphrase, preimage.MustFromAddress())
	if err != nil {
		return xdr.ContractId{}, fmt.Errorf("calculating contract ID: %w", err)
	}

	return contractID, nil
}

// createAccounts creates funded accounts for load testing.
func createAccounts(containers *LoadTestContainers, rpcURL string, count int) (
	[]*keypair.Full, []*txnbuild.SimpleAccount, []uint32, error,
) {
	var signers []*keypair.Full
	var accounts []*txnbuild.SimpleAccount
	var accountLedgers []uint32

	for i := 0; i < count; i += MaxAccountsPerTransaction {
		batchSize := MaxAccountsPerTransaction
		if i+batchSize > count {
			batchSize = count - i
		}

		// Generate keypairs for this batch
		var batchSigners []*keypair.Full
		var ops []txnbuild.Operation
		for j := 0; j < batchSize; j++ {
			kp, err := keypair.Random()
			if err != nil {
				return nil, nil, nil, fmt.Errorf("generating keypair: %w", err)
			}
			batchSigners = append(batchSigners, kp)
			ops = append(ops, &txnbuild.CreateAccount{
				Destination:   kp.Address(),
				Amount:        DefaultFundingAmount,
				SourceAccount: containers.MasterKeyPair.Address(),
			})
		}

		// Execute batch creation
		ledger, err := executeClassicOperation(containers, rpcURL, ops)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("creating accounts batch: %w", err)
		}

		// Create account objects
		for _, kp := range batchSigners {
			signers = append(signers, kp)
			accounts = append(accounts, &txnbuild.SimpleAccount{
				AccountID: kp.Address(),
				Sequence:  0,
			})
		}
		accountLedgers = append(accountLedgers, ledger)
	}

	return signers, accounts, accountLedgers, nil
}

// prepareBulkTransfers prepares bulk transfer transactions for all senders.
func prepareBulkTransfers(containers *LoadTestContainers, rpcURL string,
	bulkContractID xdr.ContractId, signers []*keypair.Full, accounts []*txnbuild.SimpleAccount,
	recipients []*keypair.Full, transfersPerTx int,
) ([]sorobanTransaction, error) {
	xlm := xdr.MustNewNativeAsset()
	sacContractID, err := xlm.ContractID(infrastructure.NetworkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("getting SAC contract ID: %w", err)
	}

	// Prepare amounts vector (same for all transactions)
	var bulkAmounts xdr.ScVec
	for i := 0; i < transfersPerTx; i++ {
		bulkAmounts = append(bulkAmounts, i128Param(0, uint64(amount.MustParse("1"))))
	}

	var transactions []sorobanTransaction
	for i := range signers {
		sender := accounts[i].GetAccountID()

		// Build recipients vector
		var bulkRecipients xdr.ScVec
		if i%2 == 0 {
			// Even: send to account addresses
			for j := i; j < i+transfersPerTx; j++ {
				recipient := accountAddressParam(recipients[j%len(recipients)].Address())
				bulkRecipients = append(bulkRecipients, recipient)
			}
		} else {
			// Odd: send to random contract addresses
			for j := 0; j < transfersPerTx; j++ {
				var contractID xdr.ContractId
				if _, err := rand.Read(contractID[:]); err != nil {
					return nil, fmt.Errorf("generating random contract ID: %w", err)
				}
				bulkRecipients = append(bulkRecipients, contractAddressParam(contractID))
			}
		}

		// Build the bulk transfer operation
		op := buildBulkTransfer(bulkContractID, sender, sacContractID, &bulkRecipients, &bulkAmounts)

		// Preflight the operation
		preflightedOp, err := preflightOperation(containers, rpcURL, accounts[i], op)
		if err != nil {
			return nil, fmt.Errorf("preflighting operation %d: %w", i, err)
		}

		// Increase resource limits to account for variability
		preflightedOp.Ext.SorobanData.Resources.DiskReadBytes *= 10
		preflightedOp.Ext.SorobanData.Resources.WriteBytes *= 10
		preflightedOp.Ext.SorobanData.Resources.Instructions *= 10
		preflightedOp.Ext.SorobanData.ResourceFee *= 10

		seqNum, err := accounts[i].GetSequenceNumber()
		if err != nil {
			return nil, fmt.Errorf("getting sequence number: %w", err)
		}

		transactions = append(transactions, sorobanTransaction{
			op:             preflightedOp,
			signer:         signers[i],
			sequenceNumber: seqNum,
		})
	}

	return transactions, nil
}

// submitTransactionsInBatches submits transactions in parallel worker batches.
func submitTransactionsInBatches(ctx context.Context, containers *LoadTestContainers, rpcURL string,
	transactions []sorobanTransaction, ledgerCount int,
) map[int32]int {
	ledgerMap := make(map[int32]int)
	var lock sync.Mutex
	var wg sync.WaitGroup

	for repetition := 0; repetition < ledgerCount; repetition++ {
		for i := 0; i < len(transactions); i += WorkerBatchSize {
			end := i + WorkerBatchSize
			if end > len(transactions) {
				end = len(transactions)
			}
			subset := transactions[i:end]

			wg.Add(1)
			go func(rep int) {
				defer wg.Done()
				if err := txSubWorker(ctx, containers, rpcURL, subset, &lock, ledgerMap, int64(rep)); err != nil {
					log.Errorf("Worker error: %v", err)
				}
			}(repetition)
		}
		wg.Wait()
	}

	return ledgerMap
}

// txSubWorker submits a batch of transactions and tracks which ledgers they land in.
func txSubWorker(ctx context.Context, containers *LoadTestContainers, rpcURL string,
	subset []sorobanTransaction, lock *sync.Mutex, ledgerMap map[int32]int, sequenceOffset int64,
) error {
	pending := make(map[string]bool)

	for _, tx := range subset {
		account := txnbuild.NewSimpleAccount(tx.signer.Address(), tx.sequenceNumber+sequenceOffset)

		builtTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &account,
			Operations:           []txnbuild.Operation{tx.op},
			BaseFee:              txnbuild.MinBaseFee * 100,
			IncrementSequenceNum: true,
			Preconditions: txnbuild.Preconditions{
				TimeBounds: txnbuild.NewTimeout(DefaultTransactionTimeout),
			},
		})
		if err != nil {
			return fmt.Errorf("building transaction: %w", err)
		}

		builtTx, err = builtTx.Sign(infrastructure.NetworkPassphrase, tx.signer)
		if err != nil {
			return fmt.Errorf("signing transaction: %w", err)
		}

		txXDR, err := builtTx.Base64()
		if err != nil {
			return fmt.Errorf("encoding transaction: %w", err)
		}

		hash, err := builtTx.HashHex(infrastructure.NetworkPassphrase)
		if err != nil {
			return fmt.Errorf("getting transaction hash: %w", err)
		}

		// Submit to RPC
		result, err := infrastructure.SubmitTransactionToRPC(containers.HTTPClient, rpcURL, txXDR)
		if err != nil {
			return fmt.Errorf("submitting transaction: %w", err)
		}
		if result.Status == entities.ErrorStatus {
			return fmt.Errorf("transaction error: %s", result.ErrorResultXDR)
		}

		pending[hash] = true
	}

	// Wait for all transactions to confirm
	return waitForTransactions(ctx, containers, rpcURL, pending, lock, ledgerMap)
}

// waitForTransactions polls for transaction confirmations.
func waitForTransactions(ctx context.Context, containers *LoadTestContainers, rpcURL string,
	pending map[string]bool, lock *sync.Mutex, ledgerMap map[int32]int,
) error {
	timeout := time.After(90 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for len(pending) > 0 {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-timeout:
			return fmt.Errorf("timeout waiting for %d transactions", len(pending))
		case <-ticker.C:
			for hash := range pending {
				result, err := infrastructure.GetTransactionFromRPC(containers.HTTPClient, rpcURL, hash)
				if err != nil {
					continue
				}
				switch result.Status {
				case entities.SuccessStatus:
					delete(pending, hash)
					lock.Lock()
					ledgerMap[int32(result.Ledger)]++
					lock.Unlock()
				case entities.FailedStatus:
					delete(pending, hash)
					log.Warnf("Transaction %s failed", hash)
				case entities.PendingStatus, entities.DuplicateStatus, entities.TryAgainLaterStatus, entities.ErrorStatus, entities.NotFoundStatus:
					// Still pending or error - continue polling
				}
			}
		}
	}
	return nil
}

// Helper functions

func findLedgerRange(ledgerMap map[int32]int) (int32, int32) {
	start, end := int32(-1), int32(-1)
	for ledgerSeq := range ledgerMap {
		if start < 0 || start > ledgerSeq {
			start = ledgerSeq
		}
		if end < 0 || ledgerSeq > end {
			end = ledgerSeq
		}
	}
	return start, end
}

func waitForEmptyLedger(ctx context.Context, containers *LoadTestContainers, rpcURL string, accountLedgers []uint32) error {
	lastAccountLedger := accountLedgers[len(accountLedgers)-1]
	target := lastAccountLedger + 1

	return waitForLedger(ctx, containers, rpcURL, target)
}

func waitForLedger(ctx context.Context, containers *LoadTestContainers, rpcURL string, target uint32) error {
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-timeout:
			return fmt.Errorf("timeout waiting for ledger %d", target)
		case <-ticker.C:
			health, err := getHealthFromRPC(containers.HTTPClient, rpcURL)
			if err != nil {
				continue
			}
			if health.LatestLedger >= target {
				return nil
			}
		}
	}
}

func getAccountEntries(ctx context.Context, containers *LoadTestContainers, rpcURL string,
	accountLedgers []uint32, count int,
) ([]xdr.LedgerEntry, error) {
	// For simplicity, we create synthetic account entries
	// In a full implementation, we'd fetch these from RPC
	var entries []xdr.LedgerEntry
	// This is a placeholder - the actual implementation would need to
	// fetch account entries from the ledgers
	log.Warnf("getAccountEntries: using placeholder implementation")
	return entries, nil
}

func mergeLedgers(containers *LoadTestContainers, rpcURL string,
	accountEntries []xdr.LedgerEntry, outputPath string, start, end uint32, transactionsPerLedger int,
) error {
	// Create output file
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer func() { _ = file.Close() }() //nolint:errcheck

	writer, err := zstd.NewWriter(file)
	if err != nil {
		return fmt.Errorf("creating zstd writer: %w", err)
	}
	defer func() { _ = writer.Close() }() //nolint:errcheck

	var merged xdr.LedgerCloseMeta
	var curCount int

	for ledgerSeq := start; ledgerSeq <= end; ledgerSeq++ {
		ledger, err := getLedgerFromRPC(containers.HTTPClient, rpcURL, ledgerSeq)
		if err != nil {
			return fmt.Errorf("getting ledger %d: %w", ledgerSeq, err)
		}

		if ledgerSeq == start {
			// First ledger: attach account fixtures
			var changes xdr.LedgerEntryChanges
			for i := range accountEntries {
				entry := accountEntries[i]
				if err := goloadtest.UpdateLedgerSeq(&entry, func(uint32) uint32 {
					return start
				}); err != nil {
					return fmt.Errorf("updating ledger seq: %w", err)
				}
				changes = append(changes, xdr.LedgerEntryChange{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: &entry,
				})
			}

			if len(changes) > 0 {
				var flag xdr.Uint32 = 1
				switch ledger.V {
				case 1:
					ledger.V1.UpgradesProcessing = append(ledger.V1.UpgradesProcessing, xdr.UpgradeEntryMeta{
						Upgrade: xdr.LedgerUpgrade{
							Type:     xdr.LedgerUpgradeTypeLedgerUpgradeFlags,
							NewFlags: &flag,
						},
						Changes: changes,
					})
				case 2:
					ledger.V2.UpgradesProcessing = append(ledger.V2.UpgradesProcessing, xdr.UpgradeEntryMeta{
						Upgrade: xdr.LedgerUpgrade{
							Type:     xdr.LedgerUpgradeTypeLedgerUpgradeFlags,
							NewFlags: &flag,
						},
						Changes: changes,
					})
				}
			}

			if err := xdr.MarshalFramed(writer, ledger); err != nil {
				return fmt.Errorf("writing first ledger: %w", err)
			}
			continue
		}

		txCount := ledger.CountTransactions()
		if txCount == 0 {
			continue
		}

		if curCount == 0 {
			merged = copyLedger(ledger)
		} else {
			ledgerDiff := int64(merged.LedgerSequence()) - int64(ledger.LedgerSequence())
			if err := goloadtest.MergeLedgers(&merged, ledger, func(cur uint32) uint32 {
				newSeq := int64(cur) + ledgerDiff
				if newSeq > math.MaxUint32 || newSeq < 0 {
					return start
				}
				return uint32(newSeq)
			}); err != nil {
				return fmt.Errorf("merging ledger %d: %w", ledgerSeq, err)
			}
		}

		curCount += txCount
		if curCount >= transactionsPerLedger {
			if err := xdr.MarshalFramed(writer, merged); err != nil {
				return fmt.Errorf("writing merged ledger: %w", err)
			}
			curCount = 0
		}
	}

	return nil
}

func copyLedger(src xdr.LedgerCloseMeta) xdr.LedgerCloseMeta {
	var dst xdr.LedgerCloseMeta
	serialized, err := src.MarshalBinary()
	if err != nil {
		// This should never happen with valid XDR
		return src
	}
	if err := dst.UnmarshalBinary(serialized); err != nil {
		// This should never happen with valid serialized data
		return src
	}
	return dst
}

// XDR helper functions

func i128Param(hi int64, lo uint64) xdr.ScVal {
	return xdr.ScVal{
		Type: xdr.ScValTypeScvI128,
		I128: &xdr.Int128Parts{
			Hi: xdr.Int64(hi),
			Lo: xdr.Uint64(lo),
		},
	}
}

func accountAddressParam(accountID string) xdr.ScVal {
	address := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: xdr.MustAddressPtr(accountID),
	}
	return xdr.ScVal{
		Type:    xdr.ScValTypeScvAddress,
		Address: &address,
	}
}

func contractAddressParam(contractID xdr.ContractId) xdr.ScVal {
	address := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractID,
	}
	return xdr.ScVal{
		Type:    xdr.ScValTypeScvAddress,
		Address: &address,
	}
}

func contractIDParam(contractID xdr.ContractId) xdr.ScAddress {
	return xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractID,
	}
}

func buildBulkTransfer(bulkContractID xdr.ContractId, sender string, sacContractID xdr.ContractId,
	recipients *xdr.ScVec, amounts *xdr.ScVec,
) *txnbuild.InvokeHostFunction {
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: contractIDParam(bulkContractID),
				FunctionName:    "bulk_transfer",
				Args: xdr.ScVec{
					accountAddressParam(sender),
					contractAddressParam(sacContractID),
					{Type: xdr.ScValTypeScvVec, Vec: &recipients},
					{Type: xdr.ScValTypeScvVec, Vec: &amounts},
				},
			},
		},
		SourceAccount: sender,
	}
}

func calculateContractID(networkPassphrase string, deployerAddress xdr.ContractIdPreimageFromAddress) (xdr.ContractId, error) {
	networkHash := xdr.Hash(sha256.Sum256([]byte(networkPassphrase)))

	hashIDPreimage := xdr.HashIdPreimage{
		Type: xdr.EnvelopeTypeEnvelopeTypeContractId,
		ContractId: &xdr.HashIdPreimageContractId{
			NetworkId: networkHash,
			ContractIdPreimage: xdr.ContractIdPreimage{
				Type:        xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
				FromAddress: &deployerAddress,
			},
		},
	}

	preimageXDR, err := hashIDPreimage.MarshalBinary()
	if err != nil {
		return xdr.ContractId{}, fmt.Errorf("marshaling preimage: %w", err)
	}

	contractIDHash := sha256.Sum256(preimageXDR)
	var contractID xdr.ContractId
	copy(contractID[:], contractIDHash[:])
	return contractID, nil
}

// RPC helper functions

func executeClassicOperation(containers *LoadTestContainers, rpcURL string,
	ops []txnbuild.Operation,
) (uint32, error) {
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        containers.MasterAccount,
		Operations:           ops,
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: true,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	if err != nil {
		return 0, fmt.Errorf("building transaction: %w", err)
	}

	tx, err = tx.Sign(infrastructure.NetworkPassphrase, containers.MasterKeyPair)
	if err != nil {
		return 0, fmt.Errorf("signing transaction: %w", err)
	}

	txXDR, err := tx.Base64()
	if err != nil {
		return 0, fmt.Errorf("encoding transaction: %w", err)
	}

	sendResult, err := infrastructure.SubmitTransactionToRPC(containers.HTTPClient, rpcURL, txXDR)
	if err != nil {
		return 0, fmt.Errorf("submitting transaction: %w", err)
	}
	if sendResult.Status == entities.ErrorStatus {
		return 0, fmt.Errorf("transaction failed: %s", sendResult.ErrorResultXDR)
	}

	if confirmErr := infrastructure.WaitForTxConfirmationRPC(containers.HTTPClient, rpcURL, sendResult.Hash, 30); confirmErr != nil {
		return 0, fmt.Errorf("waiting for confirmation: %w", confirmErr)
	}

	// Get the ledger from transaction result
	result, err := infrastructure.GetTransactionFromRPC(containers.HTTPClient, rpcURL, sendResult.Hash)
	if err != nil {
		return 0, fmt.Errorf("getting transaction result: %w", err)
	}

	return uint32(result.Ledger), nil
}

func preflightOperation(containers *LoadTestContainers, rpcURL string,
	account *txnbuild.SimpleAccount, op *txnbuild.InvokeHostFunction,
) (*txnbuild.InvokeHostFunction, error) {
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        account,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: false,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(DefaultTransactionTimeout),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("building transaction: %w", err)
	}

	txXDR, err := tx.Base64()
	if err != nil {
		return nil, fmt.Errorf("encoding transaction: %w", err)
	}

	simResult, err := infrastructure.SimulateTransactionRPC(containers.HTTPClient, rpcURL, txXDR)
	if err != nil {
		return nil, fmt.Errorf("simulating transaction: %w", err)
	}
	if simResult.Error != "" {
		return nil, fmt.Errorf("simulation failed: %s", simResult.Error)
	}

	// Create a copy with simulation results applied
	result := *op
	result.Ext = xdr.TransactionExt{
		V:           1,
		SorobanData: &simResult.TransactionData,
	}

	return &result, nil
}

func getHealthFromRPC(client *http.Client, rpcURL string) (*entities.RPCGetHealthResult, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getHealth",
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("posting to RPC: %w", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck

	var rpcResp struct {
		Result entities.RPCGetHealthResult `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return &rpcResp.Result, nil
}

func getLedgerFromRPC(client *http.Client, rpcURL string, sequence uint32) (xdr.LedgerCloseMeta, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getLedgers",
		"params": map[string]interface{}{
			"startLedger": sequence,
			"pagination": map[string]int{
				"limit": 1,
			},
		},
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("marshaling request: %w", err)
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("posting to RPC: %w", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck

	var rpcResp struct {
		Result struct {
			Ledgers []struct {
				LedgerCloseMeta string `json:"ledgerCloseMeta"`
			} `json:"ledgers"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("decoding response: %w", err)
	}

	if rpcResp.Error != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	if len(rpcResp.Result.Ledgers) == 0 {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("no ledger found at sequence %d", sequence)
	}

	var ledger xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshalBase64(rpcResp.Result.Ledgers[0].LedgerCloseMeta, &ledger); err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("unmarshaling ledger: %w", err)
	}

	return ledger, nil
}
