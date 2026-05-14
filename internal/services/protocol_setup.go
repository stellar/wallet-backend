package services

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const rpcLedgerEntryBatchSize = 200

// ProtocolSetupService classifies existing contracts on the Stellar network
// by handing batches of unclassified WASMs to each protocol's
// ProtocolValidator black box.
type ProtocolSetupService interface {
	Run(ctx context.Context, protocolIDs []string) error
}

type protocolSetupService struct {
	db                     *pgxpool.Pool
	rpcService             RPCService
	models                 *data.Models
	protocolModel          data.ProtocolsModelInterface
	protocolWasmModel      data.ProtocolWasmsModelInterface
	protocolContractsModel data.ProtocolContractsModelInterface
	specExtractor          WasmSpecExtractor
	validators             []ProtocolValidator
}

// NewProtocolSetupService creates a new ProtocolSetupService. validators must
// be in the desired first-match-wins priority order; pass the result of
// services.BuildValidators with protocol IDs sorted (or call
// services.GetAllValidatorIDs).
func NewProtocolSetupService(
	dbPool *pgxpool.Pool,
	rpcService RPCService,
	models *data.Models,
	specExtractor WasmSpecExtractor,
	validators []ProtocolValidator,
) *protocolSetupService {
	return &protocolSetupService{
		db:                     dbPool,
		rpcService:             rpcService,
		models:                 models,
		protocolModel:          models.Protocols,
		protocolWasmModel:      models.ProtocolWasms,
		protocolContractsModel: models.ProtocolContracts,
		specExtractor:          specExtractor,
		validators:             validators,
	}
}

// Run performs protocol classification for the specified protocol IDs.
func (s *protocolSetupService) Run(ctx context.Context, protocolIDs []string) error {
	if len(s.validators) == 0 {
		return fmt.Errorf("no protocol validators provided")
	}

	if s.specExtractor == nil {
		return fmt.Errorf("no spec extractor provided")
	}

	defer func() {
		if s.specExtractor == nil {
			return
		}
		if err := s.specExtractor.Close(ctx); err != nil {
			log.Ctx(ctx).Errorf("error closing spec extractor: %v", err)
		}
	}()

	// Validate that all requested protocols exist in the DB and have a
	// registered validator.
	if err := s.validateProtocolsExist(ctx, protocolIDs); err != nil {
		return fmt.Errorf("validating protocols exist: %w", err)
	}
	validatorIDs := map[string]struct{}{}
	for _, c := range s.validators {
		validatorIDs[c.ProtocolID()] = struct{}{}
	}
	for _, pid := range protocolIDs {
		if _, ok := validatorIDs[pid]; !ok {
			return fmt.Errorf("no validator provided for protocol %q", pid)
		}
	}

	// Set classification_status to in_progress.
	if err := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.protocolModel.UpdateClassificationStatus(ctx, dbTx, protocolIDs, data.StatusInProgress)
	}); err != nil {
		return fmt.Errorf("setting classification status to in_progress: %w", err)
	}

	if err := s.classify(ctx, protocolIDs); err != nil {
		// Use a fresh context for best-effort cleanup, since ctx may be cancelled.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if txErr := db.RunInTransaction(cleanupCtx, s.db, func(dbTx pgx.Tx) error {
			return s.protocolModel.UpdateClassificationStatus(cleanupCtx, dbTx, protocolIDs, data.StatusFailed)
		}); txErr != nil {
			log.Ctx(ctx).Errorf("error setting classification status to failed: %v", txErr)
		}
		return fmt.Errorf("classifying protocols: %w", err)
	}

	if err := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.protocolModel.UpdateClassificationStatus(ctx, dbTx, protocolIDs, data.StatusSuccess)
	}); err != nil {
		return fmt.Errorf("setting classification status to success: %w", err)
	}

	log.Ctx(ctx).Infof("Protocol setup completed successfully for protocols: %v", protocolIDs)
	return nil
}

// validateProtocolsExist checks that all requested protocol IDs exist in the DB.
// Protocols are registered via SQL migration files in internal/db/migrations/protocols/.
func (s *protocolSetupService) validateProtocolsExist(ctx context.Context, protocolIDs []string) error {
	found, err := s.protocolModel.GetByIDs(ctx, protocolIDs)
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
// only persists protocol_wasms.protocol_id from the returned matches.
func (s *protocolSetupService) classify(ctx context.Context, protocolIDs []string) error {
	unclassifiedWasms, err := s.protocolWasmModel.GetUnclassified(ctx)
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

	rawWasms := make([]RawWasm, 0, len(wasmBytecodes))
	for hexHash, bytecode := range wasmBytecodes {
		rawWasms = append(rawWasms, RawWasm{
			Hash:     types.HashBytea(hexHash),
			Bytecode: bytecode,
		})
	}

	// Pull protocol_contracts referencing these unclassified wasm hashes so
	// validators can enrich them inside the same dbTx.
	contracts, err := s.contractsForWasmHashes(ctx, rawWasms)
	if err != nil {
		return fmt.Errorf("loading protocol_contracts for unclassified wasms: %w", err)
	}

	// Run dispatcher and persist matches inside one tx so a validator's side
	// effects (e.g. contract_tokens metadata writes) commit atomically with
	// the protocol_wasms.protocol_id stamp.
	matches, err := s.dispatchAndPersist(ctx, rawWasms, contracts)
	if err != nil {
		return fmt.Errorf("dispatching classification: %w", err)
	}

	for protocolID := range bucketByProtocol(matches) {
		log.Ctx(ctx).Infof("Protocol %s: matched %d WASMs", protocolID, len(bucketByProtocol(matches)[protocolID]))
	}
	_ = protocolIDs // protocolIDs is used by the caller-side status updates; classify itself runs across all registered validators.
	return nil
}

// dispatchAndPersist runs DispatchClassification inside a single tx and
// updates protocol_wasms.protocol_id from the returned matches. Per-protocol
// contract_tokens writes happen inside this same tx via validator side
// effects.
func (s *protocolSetupService) dispatchAndPersist(ctx context.Context, rawWasms []RawWasm, contracts []data.ProtocolContracts) (map[types.HashBytea]string, error) {
	var matches map[types.HashBytea]string
	err := db.RunInTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		// All candidates here are unclassified, so KnownProtocolID is empty.
		var knownByHash map[types.HashBytea]string
		var dispatchErr error
		matches, dispatchErr = DispatchClassification(
			ctx, dbTx, s.specExtractor, s.validators,
			rawWasms, contracts, s.rpcService, s.models, knownByHash,
		)
		if dispatchErr != nil {
			return fmt.Errorf("dispatching: %w", dispatchErr)
		}
		for protocolID, hashes := range bucketByProtocol(matches) {
			if err := s.protocolWasmModel.BatchUpdateProtocolID(ctx, dbTx, hashes, protocolID); err != nil {
				return fmt.Errorf("updating protocol_id for %s: %w", protocolID, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("running protocol classification transaction: %w", err)
	}
	return matches, nil
}

// contractsForWasmHashes returns protocol_contracts rows whose wasm_hash is
// in the candidate set. Uses a single tx-less query through the connection
// pool since this read is not part of a CAS-guarded write.
func (s *protocolSetupService) contractsForWasmHashes(ctx context.Context, rawWasms []RawWasm) ([]data.ProtocolContracts, error) {
	if len(rawWasms) == 0 {
		return nil, nil
	}
	hashes := make([][]byte, 0, len(rawWasms))
	seen := make(map[types.HashBytea]struct{}, len(rawWasms))
	for _, w := range rawWasms {
		if _, dup := seen[w.Hash]; dup {
			continue
		}
		seen[w.Hash] = struct{}{}
		val, err := w.Hash.Value()
		if err != nil {
			log.Ctx(ctx).Debugf("contractsForWasmHashes: skipping invalid hash %q: %v", w.Hash, err)
			continue
		}
		hashes = append(hashes, val.([]byte))
	}
	if len(hashes) == 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx,
		`SELECT contract_id, wasm_hash, name, created_at FROM protocol_contracts WHERE wasm_hash = ANY($1::bytea[])`, hashes)
	if err != nil {
		return nil, fmt.Errorf("querying protocol_contracts: %w", err)
	}
	defer rows.Close()
	var out []data.ProtocolContracts
	for rows.Next() {
		var c data.ProtocolContracts
		if err := rows.Scan(&c.ContractID, &c.WasmHash, &c.Name, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning protocol_contracts row: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating protocol_contracts rows: %w", err)
	}
	return out, nil
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
