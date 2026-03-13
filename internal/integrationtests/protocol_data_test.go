// Protocol Data Integration Tests
//
// This test suite verifies that the wallet backend correctly populates the protocol_wasms
// and protocol_contracts tables during checkpoint ingestion. It validates that:
//
// 1. WASM bytecode hashes are persisted for all deployed contracts
// 2. Contract entries are linked to the correct WASM hashes
// 3. Foreign key integrity is maintained between the two tables
//
// The tests run after checkpoint population and before any fixture transactions.
package integrationtests

import (
	"context"

	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

// ProtocolDataAfterCheckpointTestSuite validates that the protocol_wasms and protocol_contracts
// tables are correctly populated during checkpoint ingestion.
type ProtocolDataAfterCheckpointTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

// TestCheckpoint_ProtocolWasms_ContainsSEP41WasmHash verifies that the SHA256 hash of
// soroban_token_contract.wasm exists in the protocol_wasms table.
func (s *ProtocolDataAfterCheckpointTestSuite) TestCheckpoint_ProtocolWasms_ContainsSEP41WasmHash() {
	sep41Hash, err := infrastructure.ComputeWasmHash("soroban_token_contract.wasm")
	s.Require().NoError(err, "computing SEP-41 WASM hash")

	found, err := s.testEnv.Containers.GetProtocolWasmByHash(context.Background(), sep41Hash)
	s.Require().NoError(err, "querying protocol_wasms for SEP-41 hash")
	s.Require().True(found, "SEP-41 WASM hash %s should exist in protocol_wasms", sep41Hash)
}

// TestCheckpoint_ProtocolWasms_ContainsHolderWasmHash verifies that the SHA256 hash of
// soroban_increment_contract.wasm exists in the protocol_wasms table.
func (s *ProtocolDataAfterCheckpointTestSuite) TestCheckpoint_ProtocolWasms_ContainsHolderWasmHash() {
	holderHash, err := infrastructure.ComputeWasmHash("soroban_increment_contract.wasm")
	s.Require().NoError(err, "computing holder WASM hash")

	found, err := s.testEnv.Containers.GetProtocolWasmByHash(context.Background(), holderHash)
	s.Require().NoError(err, "querying protocol_wasms for holder hash")
	s.Require().True(found, "Holder WASM hash %s should exist in protocol_wasms", holderHash)
}

// TestCheckpoint_ProtocolWasms_HasExpectedMinimumCount verifies that the protocol_wasms
// table has at least 2 rows (one for each deployed WASM).
func (s *ProtocolDataAfterCheckpointTestSuite) TestCheckpoint_ProtocolWasms_HasExpectedMinimumCount() {
	count, err := s.testEnv.Containers.GetProtocolWasmCount(context.Background())
	s.Require().NoError(err, "counting protocol_wasms")
	s.Require().GreaterOrEqual(count, 2, "protocol_wasms should have at least 2 entries (SEP-41 + holder)")
}

// TestCheckpoint_ProtocolContracts_SEP41LinkedToCorrectWasm verifies that the SEP-41
// contract entry in protocol_contracts is linked to the correct WASM hash.
func (s *ProtocolDataAfterCheckpointTestSuite) TestCheckpoint_ProtocolContracts_SEP41LinkedToCorrectWasm() {
	expectedHash, err := infrastructure.ComputeWasmHash("soroban_token_contract.wasm")
	s.Require().NoError(err, "computing SEP-41 WASM hash")

	wasmHash, found, err := s.testEnv.Containers.GetProtocolContractByID(context.Background(), s.testEnv.SEP41ContractAddress)
	s.Require().NoError(err, "querying protocol_contracts for SEP-41 contract")
	s.Require().True(found, "SEP-41 contract %s should exist in protocol_contracts", s.testEnv.SEP41ContractAddress)
	s.Require().Equal(expectedHash, wasmHash, "SEP-41 contract should be linked to the correct WASM hash")
}

// TestCheckpoint_ProtocolContracts_HolderLinkedToCorrectWasm verifies that the holder
// contract entry in protocol_contracts is linked to the correct WASM hash.
func (s *ProtocolDataAfterCheckpointTestSuite) TestCheckpoint_ProtocolContracts_HolderLinkedToCorrectWasm() {
	expectedHash, err := infrastructure.ComputeWasmHash("soroban_increment_contract.wasm")
	s.Require().NoError(err, "computing holder WASM hash")

	wasmHash, found, err := s.testEnv.Containers.GetProtocolContractByID(context.Background(), s.testEnv.HolderContractAddress)
	s.Require().NoError(err, "querying protocol_contracts for holder contract")
	s.Require().True(found, "Holder contract %s should exist in protocol_contracts", s.testEnv.HolderContractAddress)
	s.Require().Equal(expectedHash, wasmHash, "Holder contract should be linked to the correct WASM hash")
}

// TestCheckpoint_ProtocolContracts_HasExpectedMinimumCount verifies that the protocol_contracts
// table has at least 2 rows (one for each deployed contract).
func (s *ProtocolDataAfterCheckpointTestSuite) TestCheckpoint_ProtocolContracts_HasExpectedMinimumCount() {
	count, err := s.testEnv.Containers.GetProtocolContractCount(context.Background())
	s.Require().NoError(err, "counting protocol_contracts")
	s.Require().GreaterOrEqual(count, 2, "protocol_contracts should have at least 2 entries (SEP-41 + holder)")
}

// TestCheckpoint_ProtocolContracts_NoOrphanedWasmReferences verifies that all contracts
// in protocol_contracts have a valid foreign key reference to protocol_wasms.
func (s *ProtocolDataAfterCheckpointTestSuite) TestCheckpoint_ProtocolContracts_NoOrphanedWasmReferences() {
	orphanCount, err := s.testEnv.Containers.GetProtocolContractsWithOrphanedWasmHash(context.Background())
	s.Require().NoError(err, "checking for orphaned wasm references")
	s.Require().Equal(0, orphanCount, "all protocol_contracts should have valid wasm_hash references in protocol_wasms")
}
