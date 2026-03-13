// checkpoint_helpers.go provides helper functions for checkpoint protocol data integration testing.
package infrastructure

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/stellar/go-stellar-sdk/strkey"
)

// GetProtocolWasmCount counts the number of rows in the protocol_wasms table.
func (s *SharedContainers) GetProtocolWasmCount(ctx context.Context) (int, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var count int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM protocol_wasms`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting protocol_wasms: %w", err)
	}

	return count, nil
}

// GetProtocolWasmByHash checks if a specific WASM hash exists in the protocol_wasms table.
// The wasmHashHex parameter should be a hex-encoded SHA256 hash.
func (s *SharedContainers) GetProtocolWasmByHash(ctx context.Context, wasmHashHex string) (bool, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return false, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return false, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	hashBytes, err := hex.DecodeString(wasmHashHex)
	if err != nil {
		return false, fmt.Errorf("decoding hex hash: %w", err)
	}

	var exists bool
	query := `SELECT EXISTS (SELECT 1 FROM protocol_wasms WHERE wasm_hash = $1)`
	err = db.QueryRowContext(ctx, query, hashBytes).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("checking protocol_wasms for hash %s: %w", wasmHashHex, err)
	}

	return exists, nil
}

// GetProtocolContractCount counts the number of rows in the protocol_contracts table.
func (s *SharedContainers) GetProtocolContractCount(ctx context.Context) (int, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var count int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM protocol_contracts`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting protocol_contracts: %w", err)
	}

	return count, nil
}

// GetProtocolContractByID looks up a protocol contract by its C... address and returns
// the linked wasm_hash as a hex string. Returns found=false if the contract is not in the table.
func (s *SharedContainers) GetProtocolContractByID(ctx context.Context, contractAddress string) (wasmHashHex string, found bool, err error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return "", false, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return "", false, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	contractIDBytes, err := strkey.Decode(strkey.VersionByteContract, contractAddress)
	if err != nil {
		return "", false, fmt.Errorf("decoding contract address %s: %w", contractAddress, err)
	}

	var hashHex string
	query := `SELECT encode(wasm_hash, 'hex') FROM protocol_contracts WHERE contract_id = $1`
	err = db.QueryRowContext(ctx, query, contractIDBytes).Scan(&hashHex)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("querying protocol_contracts for %s: %w", contractAddress, err)
	}

	return hashHex, true, nil
}

// GetProtocolContractsWithOrphanedWasmHash counts protocol contracts whose wasm_hash
// does not have a corresponding entry in protocol_wasms (FK integrity check).
func (s *SharedContainers) GetProtocolContractsWithOrphanedWasmHash(ctx context.Context) (int, error) {
	dbURL, err := s.GetWalletDBConnectionString(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting database connection string: %w", err)
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var count int
	query := `
		SELECT COUNT(*)
		FROM protocol_contracts pc
		LEFT JOIN protocol_wasms pw ON pc.wasm_hash = pw.wasm_hash
		WHERE pw.wasm_hash IS NULL
	`
	err = db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting orphaned wasm references: %w", err)
	}

	return count, nil
}

// ComputeWasmHash reads a WASM file from the testdata directory and returns its SHA256 hash as a hex string.
func ComputeWasmHash(wasmFilename string) (string, error) {
	_, callerFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("failed to get caller information")
	}
	dir := filepath.Dir(callerFile)

	wasmBytes, err := os.ReadFile(filepath.Join(dir, "testdata", wasmFilename))
	if err != nil {
		return "", fmt.Errorf("reading WASM file %s: %w", wasmFilename, err)
	}

	hash := sha256.Sum256(wasmBytes)
	return hex.EncodeToString(hash[:]), nil
}
