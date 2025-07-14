package indexer

import (
	"sync"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func Test_IndexerBuffer_PushParticipantTransaction_and_Getters(t *testing.T) {
	t.Run("🟢sequential_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := ingest.LedgerTransaction{Hash: xdr.Hash{1, 1, 1, 1}}
		tx2 := ingest.LedgerTransaction{Hash: xdr.Hash{2, 2, 2, 2}}
		tx1Hash := tx1.Hash.HexString()
		tx2Hash := tx2.Hash.HexString()

		op1 := xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypeCreateAccount}}
		op2 := xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypeCreateAccount}}

		indexerBuffer.PushParticipantTransaction("alice", tx1)
		indexerBuffer.PushParticipantTransaction("alice", tx2)
		indexerBuffer.PushParticipantTransaction("bob", tx2)
		indexerBuffer.PushParticipantTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally

		indexerBuffer.PushParticipantOperation("alice", 1, op1, tx1, 0)
		indexerBuffer.PushParticipantOperation("bob", 2, op2, tx2, 0)
		indexerBuffer.PushParticipantOperation("chuck", 2, op2, tx2, 0)
		indexerBuffer.PushParticipantOperation("chuck", 2, op2, tx2, 0) // <--- duplicate operation ID is a no-op because we use a Set internally

		// Assert participants txHashes
		wantTxHashesByParticipant := map[string]set.Set[string]{
			"alice": set.NewSet(tx1Hash, tx2Hash),
			"bob":   set.NewSet(tx2Hash),
			"chuck": set.NewSet(tx2Hash),
		}
		assert.Equal(t, wantTxHashesByParticipant, indexerBuffer.txHashesByParticipant)

		// Assert GetParticipantTransactions
		assert.ElementsMatch(t, []ingest.LedgerTransaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
		assert.ElementsMatch(t, []ingest.LedgerTransaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))
		assert.ElementsMatch(t, []ingest.LedgerTransaction{tx2}, indexerBuffer.GetParticipantTransactions("chuck"))

		// Assert txByHash
		wantTxByHash := map[string]ingest.LedgerTransaction{
			tx1Hash: tx1,
			tx2Hash: tx2,
		}
		assert.Equal(t, wantTxByHash, indexerBuffer.txByHash)

		// Assert Participants
		assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.Participants)

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []ingest.LedgerTransaction{tx1, tx2}, indexerBuffer.GetAllTransactions())

		// Assert operations
		wantOpByID := map[int64]xdr.Operation{
			1: op1,
			2: op2,
		}
		assert.Equal(t, wantOpByID, indexerBuffer.opByID)

		// Assert GetOperationIndex
		assert.Equal(t, uint32(0), indexerBuffer.GetOperationIndex(1))
		assert.Equal(t, uint32(0), indexerBuffer.GetOperationIndex(2))

		// Assert GetOperationTransaction
		assert.Equal(t, tx1, indexerBuffer.GetOperationTransaction(1))
		assert.Equal(t, tx2, indexerBuffer.GetOperationTransaction(2))

		// Assert GetParticipantOperations
		wantAliceOps := map[int64]xdr.Operation{
			1: op1,
		}
		assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("alice"))
		wantBobOps := map[int64]xdr.Operation{
			2: op2,
		}
		assert.Equal(t, wantBobOps, indexerBuffer.GetParticipantOperations("bob"))
		wantChuckOps := map[int64]xdr.Operation{
			2: op2,
		}
		assert.Equal(t, wantChuckOps, indexerBuffer.GetParticipantOperations("chuck"))
	})

	t.Run("🟢concurrent_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := ingest.LedgerTransaction{Hash: xdr.Hash{1, 1, 1, 1}}
		tx1Hash := tx1.Hash.HexString()

		tx2 := ingest.LedgerTransaction{Hash: xdr.Hash{2, 2, 2, 2}}
		tx2Hash := tx2.Hash.HexString()

		op1 := xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypeCreateAccount}}
		op2 := xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypeCreateAccount}}

		// Concurrent push operations
		wg := sync.WaitGroup{}
		wg.Add(8)
		go func() {
			indexerBuffer.PushParticipantTransaction("alice", tx1)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantTransaction("alice", tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantTransaction("bob", tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("alice", 1, op1, tx1, 0)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("bob", 2, op2, tx2, 0)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("chuck", 2, op2, tx2, 0)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("chuck", 2, op2, tx2, 0) // <--- duplicate operation ID is a no-op because we use a Set internally
			wg.Done()
		}()
		wg.Wait()

		// Concurrent getter operations
		wg = sync.WaitGroup{}
		wg.Add(13)

		// GetParticipantTransactions
		go func() {
			assert.ElementsMatch(t, []ingest.LedgerTransaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []ingest.LedgerTransaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []ingest.LedgerTransaction{tx2}, indexerBuffer.GetParticipantTransactions("chuck"))
			wg.Done()
		}()

		// GetParticipantOperations
		go func() {
			wantAliceOps := map[int64]xdr.Operation{
				1: op1,
			}
			assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("alice"))
			wg.Done()
		}()
		go func() {
			wantAliceOps := map[int64]xdr.Operation{
				2: op2,
			}
			assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("bob"))
			wg.Done()
		}()
		go func() {
			wantAliceOps := map[int64]xdr.Operation{
				2: op2,
			}
			assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("chuck"))
			wg.Done()
		}()

		// Assert GetNumberOfTransactions
		go func() {
			assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())
			wg.Done()
		}()

		// Assert Participants
		go func() {
			assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.Participants)
			wg.Done()
		}()

		// Assert GetAllTransactions
		go func() {
			assert.ElementsMatch(t, []ingest.LedgerTransaction{tx1, tx2}, indexerBuffer.GetAllTransactions())
			wg.Done()
		}()

		// Assert GetOperationIndex
		go func() {
			assert.Equal(t, uint32(0), indexerBuffer.GetOperationIndex(1))
			wg.Done()
		}()
		go func() {
			assert.Equal(t, uint32(0), indexerBuffer.GetOperationIndex(2))
			wg.Done()
		}()

		// Assert GetOperationTransaction
		go func() {
			assert.Equal(t, tx1, indexerBuffer.GetOperationTransaction(1))
			wg.Done()
		}()
		go func() {
			assert.Equal(t, tx2, indexerBuffer.GetOperationTransaction(2))
			wg.Done()
		}()

		// Wait for all getter operations to complete
		wg.Wait()

		// Assert txByHash
		wantTxByHash := map[string]ingest.LedgerTransaction{
			tx1Hash: tx1,
			tx2Hash: tx2,
		}
		assert.Equal(t, wantTxByHash, indexerBuffer.txByHash)

		// Assert participants txHashes
		wantTxHashesByParticipant := map[string]set.Set[string]{
			"alice": set.NewSet(tx1Hash, tx2Hash),
			"bob":   set.NewSet(tx2Hash),
			"chuck": set.NewSet(tx2Hash),
		}
		assert.Equal(t, wantTxHashesByParticipant, indexerBuffer.txHashesByParticipant)

		// Assert operations
		wantOpByID := map[int64]xdr.Operation{
			1: op1,
			2: op2,
		}
		assert.Equal(t, wantOpByID, indexerBuffer.opByID)
	})
}
