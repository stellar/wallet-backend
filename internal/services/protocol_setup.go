package services

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const rpcLedgerEntryBatchSize = 200

// ProtocolSetupService classifies existing contracts on the Stellar network
// by comparing WASM bytecodes against protocol-specific interface definitions.
type ProtocolSetupService interface {
	Run(ctx context.Context, protocolIDs []string) error
}

type protocolSetupService struct {
	db                db.ConnectionPool
	rpcService        RPCService
	protocolModel     data.ProtocolsModelInterface
	protocolWasmModel data.ProtocolWasmsModelInterface
	specExtractor     WasmSpecExtractor
	validators        []ProtocolValidator
}

// ProtocolCursorName returns the ingest_store cursor key for a given protocol and cursor type.
func ProtocolCursorName(protocolID, cursorType string) string {
	return fmt.Sprintf("protocol_%s_%s_cursor", protocolID, cursorType)
}

// NewProtocolSetupService creates a new ProtocolSetupService.
func NewProtocolSetupService(
	dbPool db.ConnectionPool,
	rpcService RPCService,
	protocolModel data.ProtocolsModelInterface,
	protocolWasmModel data.ProtocolWasmsModelInterface,
	specExtractor WasmSpecExtractor,
	validators []ProtocolValidator,
) *protocolSetupService {
	return &protocolSetupService{
		db:                dbPool,
		rpcService:        rpcService,
		protocolModel:     protocolModel,
		protocolWasmModel: protocolWasmModel,
		specExtractor:     specExtractor,
		validators:        validators,
	}
}

// Run performs protocol classification for the specified protocol IDs.
func (s *protocolSetupService) Run(ctx context.Context, protocolIDs []string) error {
	if len(s.validators) == 0 {
		return fmt.Errorf("no protocol validators provided")
	}

	// Step 1: Validate that validators match requested protocol IDs
	validatorsByProtocol := make(map[string]ProtocolValidator, len(s.validators))
	for _, v := range s.validators {
		validatorsByProtocol[v.ProtocolID()] = v
	}
	for _, pid := range protocolIDs {
		if _, ok := validatorsByProtocol[pid]; !ok {
			return fmt.Errorf("no validator found for protocol %q", pid)
		}
	}

	defer func() {
		if err := s.specExtractor.Close(ctx); err != nil {
			log.Ctx(ctx).Errorf("error closing spec extractor: %v", err)
		}
	}()

	// Step 2: Validate that all requested protocols exist in the DB
	if err := s.validateProtocolsExist(ctx, protocolIDs); err != nil {
		return fmt.Errorf("validating protocols exist: %w", err)
	}

	// Step 3: Set classification_status to in_progress
	if err := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		return s.protocolModel.UpdateClassificationStatus(ctx, dbTx, protocolIDs, data.StatusInProgress)
	}); err != nil {
		return fmt.Errorf("setting classification status to in_progress: %w", err)
	}

	// Run classification, setting status to failed on error
	if err := s.classify(ctx, protocolIDs, validatorsByProtocol); err != nil {
		// Use a fresh context for best-effort cleanup, since ctx may be cancelled
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if txErr := db.RunInPgxTransaction(cleanupCtx, s.db, func(dbTx pgx.Tx) error {
			return s.protocolModel.UpdateClassificationStatus(cleanupCtx, dbTx, protocolIDs, data.StatusFailed)
		}); txErr != nil {
			log.Ctx(ctx).Errorf("error setting classification status to failed: %v", txErr)
		}
		return fmt.Errorf("classifying protocols: %w", err)
	}

	// Set classification_status to success
	if err := db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
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

// classify queries protocol_wasms for unclassified hashes, fetches bytecodes via RPC,
// then classifies them against the provided validators.
func (s *protocolSetupService) classify(ctx context.Context, protocolIDs []string, validatorsByProtocol map[string]ProtocolValidator) error {
	// Get unclassified WASM hashes from the DB
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

	// Fetch bytecodes via RPC
	wasmBytecodes, err := s.fetchWasmBytecodes(ctx, hexHashes)
	if err != nil {
		return fmt.Errorf("fetching wasm bytecodes via RPC: %w", err)
	}
	log.Ctx(ctx).Infof("Fetched %d WASM bytecodes from RPC (out of %d requested)", len(wasmBytecodes), len(hexHashes))

	// Classify each WASM with a fetched bytecode
	matchedHashes := make(map[string][]types.HashBytea)
	for wasmHash, bytecode := range wasmBytecodes {
		specEntries, extractErr := s.specExtractor.ExtractSpec(ctx, bytecode)
		if extractErr != nil {
			log.Ctx(ctx).Warnf("Failed to extract spec from WASM %s: %v", wasmHash, extractErr)
			continue
		}

		for _, pid := range protocolIDs {
			validator := validatorsByProtocol[pid]
			if validator.Validate(specEntries) {
				matchedHashes[pid] = append(matchedHashes[pid], types.HashBytea(wasmHash))
				break // A WASM matches at most one protocol
			}
		}
	}

	// Persist results in a single transaction
	err = db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		for protocolID, hashes := range matchedHashes {
			if err := s.protocolWasmModel.BatchUpdateProtocolID(ctx, dbTx, hashes, protocolID); err != nil {
				return fmt.Errorf("updating protocol_id for %s: %w", protocolID, err)
			}
			log.Ctx(ctx).Infof("Protocol %s: matched %d WASMs", protocolID, len(hashes))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("persisting classification results: %w", err)
	}

	return nil
}

// fetchWasmBytecodes fetches WASM bytecodes from RPC for the given hex hashes.
// Returns a map of hexHash -> bytecode. Hashes not found in the ledger (expired/evicted) are omitted.
func (s *protocolSetupService) fetchWasmBytecodes(ctx context.Context, hexHashes []string) (map[string][]byte, error) {
	// Build base64 ledger keys
	base64Keys := make([]string, 0, len(hexHashes))

	for _, hexHash := range hexHashes {
		hashBytes, err := hex.DecodeString(hexHash)
		if err != nil {
			return nil, fmt.Errorf("decoding hex hash %s: %w", hexHash, err)
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

	// Batch keys into groups and fetch from RPC
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

	// Log warnings for hashes not found (expired/evicted)
	for _, hexHash := range hexHashes {
		if _, found := foundKeys[hexHash]; !found {
			log.Ctx(ctx).Warnf("WASM hash %s not found in ledger (possibly expired/evicted)", hexHash)
		}
	}

	return result, nil
}
