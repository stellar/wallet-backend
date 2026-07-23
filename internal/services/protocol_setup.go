package services

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const rpcLedgerEntryBatchSize = 200

// protocolSetupService classifies existing contracts on the Stellar network
// by handing batches of unclassified WASMs to each protocol's
// ProtocolValidator black box.
type protocolSetupService struct {
	db                              *pgxpool.Pool
	rpcService                      RPCService
	models                          *data.Models
	specExtractor                   WasmSpecExtractor
	validators                      []ProtocolValidator
	wasmClassificationFailuresTotal *prometheus.CounterVec
}

// NewProtocolSetupService creates a new ProtocolSetupService. validators must
// be in the desired first-match-wins priority order; pass the result of
// services.BuildValidators with protocol IDs sorted (or call
// services.GetAllValidatorIDs). wasmClassificationFailuresTotal may be nil;
// when set, it is incremented at each WASM classification failure inside
// DispatchClassification so operator-driven setup runs surface the same signal
// as live ingest.
func NewProtocolSetupService(
	dbPool *pgxpool.Pool,
	rpcService RPCService,
	models *data.Models,
	validators []ProtocolValidator,
	wasmClassificationFailuresTotal *prometheus.CounterVec,
) *protocolSetupService {
	return &protocolSetupService{
		db:                              dbPool,
		rpcService:                      rpcService,
		models:                          models,
		specExtractor:                   NewWasmSpecExtractor(),
		validators:                      validators,
		wasmClassificationFailuresTotal: wasmClassificationFailuresTotal,
	}
}

// Run performs protocol classification for the specified protocol IDs.
func (s *protocolSetupService) Run(ctx context.Context, protocolIDs []string) error {
	if len(s.validators) == 0 {
		return fmt.Errorf("no protocol validators provided")
	}

	defer func() {
		if err := s.specExtractor.Close(ctx); err != nil {
			log.Ctx(ctx).Errorf("error closing spec extractor: %v", err)
		}
	}()

	// Validate that all requested protocols exist in the DB and have a
	// registered validator.
	if err := s.validateProtocolsExist(ctx, protocolIDs); err != nil {
		return fmt.Errorf("validating protocols exist: %w", err)
	}

	// Set classification_status to in_progress.
	if err := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.models.Protocols.UpdateClassificationStatus(ctx, dbTx, protocolIDs, data.StatusInProgress)
	}); err != nil {
		return fmt.Errorf("setting classification status to in_progress: %w", err)
	}

	if err := s.classify(ctx); err != nil {
		// On shutdown ctx is cancelled; detach (keeping ctx values) so the
		// failed-status write still lands and the status reaches a terminal state.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if txErr := db.RunInTransaction(cleanupCtx, s.db, func(dbTx pgx.Tx) error {
			return s.models.Protocols.UpdateClassificationStatus(cleanupCtx, dbTx, protocolIDs, data.StatusFailed)
		}); txErr != nil {
			log.Ctx(ctx).Errorf("error setting classification status to failed: %v", txErr)
		}
		return fmt.Errorf("classifying protocols: %w", err)
	}

	if err := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.models.Protocols.UpdateClassificationStatus(ctx, dbTx, protocolIDs, data.StatusSuccess)
	}); err != nil {
		return fmt.Errorf("setting classification status to success: %w", err)
	}

	log.Ctx(ctx).Infof("Protocol setup completed successfully for protocols: %v", protocolIDs)
	return nil
}

// validateProtocolsExist checks that all requested protocol IDs exist in the DB.
// Protocols are registered via SQL migration files in internal/db/migrations/protocols/.
func (s *protocolSetupService) validateProtocolsExist(ctx context.Context, protocolIDs []string) error {
	found, err := s.models.Protocols.GetByIDs(ctx, protocolIDs)
	if err != nil {
		return fmt.Errorf("querying protocols: %w", err)
	}

	foundSet := make(map[string]struct{}, len(found))
	for _, p := range found {
		foundSet[p.ID] = struct{}{}
	}

	var missing []string
	for _, pid := range protocolIDs {
		if _, ok := foundSet[pid]; !ok {
			missing = append(missing, pid)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("protocols not found in DB %v — ensure a protocol migration SQL file exists in internal/db/migrations/protocols/", missing)
	}

	return nil
}

// classify pulls unclassified WASMs from protocol_wasms, fetches their
// bytecodes via RPC, and hands the batch to the dispatcher. The validators
// own everything that happens during classification — signature checks plus
// any side writes (e.g. SEP-41 contract_tokens metadata). The framework
// only persists protocol_wasms.protocol_id from the matches.
func (s *protocolSetupService) classify(ctx context.Context) error {
	unclassifiedWasms, err := s.models.ProtocolWasms.GetUnclassified(ctx)
	if err != nil {
		return fmt.Errorf("getting unclassified wasm hashes: %w", err)
	}
	if len(unclassifiedWasms) == 0 {
		log.Ctx(ctx).Info("No unclassified WASMs found, nothing to classify")
		return nil
	}

	hexHashes := make([]string, len(unclassifiedWasms))
	for i, w := range unclassifiedWasms {
		hexHashes[i] = w.WasmHash.String()
	}
	log.Ctx(ctx).Infof("Found %d unclassified WASM hashes to classify", len(hexHashes))

	wasmBytecodes, err := s.fetchWasmBytecodes(ctx, hexHashes)
	if err != nil {
		return fmt.Errorf("fetching wasm bytecodes via RPC: %w", err)
	}
	log.Ctx(ctx).Infof("Fetched %d WASM bytecodes from RPC (out of %d requested)", len(wasmBytecodes), len(hexHashes))

	bytecodesByHash := make(map[types.HashBytea][]byte, len(wasmBytecodes))
	for hexHash, bytecode := range wasmBytecodes {
		bytecodesByHash[types.HashBytea(hexHash)] = bytecode
	}

	// Pull protocol_contracts referencing these unclassified wasm hashes so
	// validators can enrich them inside the same dbTx.
	wasmHashes := make([]types.HashBytea, 0, len(bytecodesByHash))
	for hash := range bytecodesByHash {
		wasmHashes = append(wasmHashes, hash)
	}
	contracts, err := s.models.ProtocolContracts.GetByWasmHashes(ctx, wasmHashes)
	if err != nil {
		return fmt.Errorf("loading protocol_contracts for unclassified wasms: %w", err)
	}

	// Prepare classification (matching plus any RPC-sourced enrichment
	// prefetch) before opening any transaction, then persist matches and each
	// validator's DB writes inside one tx so they commit atomically with the
	// protocol_wasms.protocol_id stamp. No RPC handle is available past this
	// point.
	byProtocol, err := s.prepareAndPersist(ctx, bytecodesByHash, contracts)
	if err != nil {
		return fmt.Errorf("dispatching classification: %w", err)
	}

	for pid, hashes := range byProtocol {
		log.Ctx(ctx).Infof("Protocol %s: matched %d WASMs", pid, len(hashes))
	}
	return nil
}

// prepareAndPersist runs PrepareClassification (pure matching plus RPC
// prefetch) before opening any transaction, then applies the resulting plan
// and updates protocol_wasms.protocol_id inside a single tx, returning the
// matches grouped per protocol. Per-protocol DB writes (e.g. contract_tokens
// metadata) happen inside this same tx via ApplyClassificationPlan; no RPC
// call happens once the transaction opens.
func (s *protocolSetupService) prepareAndPersist(ctx context.Context, bytecodesByHash map[types.HashBytea][]byte, contracts []data.ProtocolContracts) (map[string][]types.HashBytea, error) {
	// All candidates here are unclassified, so KnownProtocolID is empty.
	var knownByHash map[types.HashBytea]string
	plan, err := PrepareClassification(
		ctx, s.specExtractor, s.validators,
		bytecodesByHash, contracts, s.rpcService, knownByHash,
		s.wasmClassificationFailuresTotal,
	)
	if err != nil {
		return nil, fmt.Errorf("preparing classification: %w", err)
	}

	var byProtocol map[string][]types.HashBytea
	err = db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		if applyErr := ApplyClassificationPlan(ctx, dbTx, s.models, plan, s.wasmClassificationFailuresTotal); applyErr != nil {
			return fmt.Errorf("applying classification: %w", applyErr)
		}
		byProtocol = bucketByProtocol(plan.Matches)
		for protocolID, hashes := range byProtocol {
			if err := s.models.ProtocolWasms.BatchUpdateProtocolID(ctx, dbTx, hashes, protocolID); err != nil {
				return fmt.Errorf("updating protocol_id for %s: %w", protocolID, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("running protocol classification transaction: %w", err)
	}
	return byProtocol, nil
}

// bucketByProtocol groups matched wasm hashes by protocol id.
func bucketByProtocol(matches map[types.HashBytea]string) map[string][]types.HashBytea {
	out := make(map[string][]types.HashBytea)
	for hash, pid := range matches {
		out[pid] = append(out[pid], hash)
	}
	return out
}

// fetchWasmBytecodes fetches WASM bytecodes from RPC for the given hex hashes.
// Returns a map of hexHash -> bytecode. Hashes not found in the ledger (expired/evicted) are omitted.
func (s *protocolSetupService) fetchWasmBytecodes(ctx context.Context, hexHashes []string) (map[string][]byte, error) {
	base64Keys := make([]string, 0, len(hexHashes))

	for _, hexHash := range hexHashes {
		hashBytes, err := hex.DecodeString(hexHash)
		if err != nil {
			return nil, fmt.Errorf("decoding hex hash %s: %w", hexHash, err)
		}
		if len(hashBytes) != 32 {
			return nil, fmt.Errorf("invalid hash length for %s: got %d bytes, want 32", hexHash, len(hashBytes))
		}

		var hash xdr.Hash
		copy(hash[:], hashBytes)

		ledgerKey := xdr.LedgerKey{
			Type:         xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.LedgerKeyContractCode{Hash: hash},
		}

		base64Key, err := ledgerKey.MarshalBinaryBase64()
		if err != nil {
			return nil, fmt.Errorf("marshaling ledger key for hash %s: %w", hexHash, err)
		}

		base64Keys = append(base64Keys, base64Key)
	}

	result := make(map[string][]byte, len(hexHashes))
	foundKeys := make(map[string]struct{})

	for i := 0; i < len(base64Keys); i += rpcLedgerEntryBatchSize {
		end := i + rpcLedgerEntryBatchSize
		if end > len(base64Keys) {
			end = len(base64Keys)
		}
		batch := base64Keys[i:end]

		rpcResult, err := s.rpcService.GetLedgerEntries(batch)
		if err != nil {
			return nil, fmt.Errorf("calling GetLedgerEntries: %w", err)
		}

		for _, entry := range rpcResult.Entries {
			var ledgerEntryData xdr.LedgerEntryData
			if err := xdr.SafeUnmarshalBase64(entry.DataXDR, &ledgerEntryData); err != nil {
				return nil, fmt.Errorf("unmarshaling ledger entry data: %w", err)
			}

			if ledgerEntryData.Type != xdr.LedgerEntryTypeContractCode {
				log.Ctx(ctx).Warnf("unexpected ledger entry type %v in GetLedgerEntries response, skipping", ledgerEntryData.Type)
				continue
			}

			codeEntry := ledgerEntryData.MustContractCode()
			hexHash := hex.EncodeToString(codeEntry.Hash[:])
			result[hexHash] = codeEntry.Code
			foundKeys[hexHash] = struct{}{}
		}
	}

	for _, hexHash := range hexHashes {
		if _, found := foundKeys[hexHash]; !found {
			log.Ctx(ctx).Warnf("WASM hash %s not found in ledger (possibly expired/evicted)", hexHash)
		}
	}

	return result, nil
}
