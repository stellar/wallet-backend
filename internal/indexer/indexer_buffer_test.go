package indexer

import (
	"sync"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func Test_IndexerBuffer_PushParticipantTransaction_and_Getters(t *testing.T) {
	t.Run("🟢sequential_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		indexerBuffer.PushParticipantTransaction("alice", tx1)
		indexerBuffer.PushParticipantTransaction("alice", tx2)
		indexerBuffer.PushParticipantTransaction("bob", tx2)
		indexerBuffer.PushParticipantTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally

		indexerBuffer.PushParticipantOperation("alice", op1, tx1)
		indexerBuffer.PushParticipantOperation("bob", op2, tx2)
		indexerBuffer.PushParticipantOperation("chuck", op2, tx2)
		indexerBuffer.PushParticipantOperation("chuck", op2, tx2) // <--- duplicate operation ID is a no-op because we use a Set internally

		// Assert participants txHashes
		wantTxHashesByParticipant := map[string]set.Set[string]{
			"alice": set.NewSet("tx_hash_1", "tx_hash_2"),
			"bob":   set.NewSet("tx_hash_2"),
			"chuck": set.NewSet("tx_hash_2"),
		}
		assert.Equal(t, wantTxHashesByParticipant, indexerBuffer.txHashesByParticipant)

		// Assert GetParticipantTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("chuck"))

		// Assert txByHash
		wantTxByHash := map[string]types.Transaction{
			"tx_hash_1": tx1,
			"tx_hash_2": tx2,
		}
		assert.Equal(t, wantTxByHash, indexerBuffer.txByHash)

		// Assert Participants
		assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.Participants)

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetAllTransactions())

		// Assert operations
		wantOpByID := map[int64]types.Operation{
			1: op1,
			2: op2,
		}
		assert.Equal(t, wantOpByID, indexerBuffer.opByID)

		// Assert GetParticipantOperations
		wantAliceOps := map[int64]types.Operation{
			1: op1,
		}
		assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("alice"))
		wantBobOps := map[int64]types.Operation{
			2: op2,
		}
		assert.Equal(t, wantBobOps, indexerBuffer.GetParticipantOperations("bob"))
		wantChuckOps := map[int64]types.Operation{
			2: op2,
		}
		assert.Equal(t, wantChuckOps, indexerBuffer.GetParticipantOperations("chuck"))
	})

	t.Run("🟢concurrent_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

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
			indexerBuffer.PushParticipantOperation("alice", op1, tx1)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("bob", op2, tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("chuck", op2, tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("chuck", op2, tx2) // <--- duplicate operation ID is a no-op because we use a Set internally
			wg.Done()
		}()
		wg.Wait()

		// Concurrent getter operations
		wg = sync.WaitGroup{}
		wg.Add(9)

		// GetParticipantTransactions
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("chuck"))
			wg.Done()
		}()

		// GetParticipantOperations
		go func() {
			wantAliceOps := map[int64]types.Operation{
				1: op1,
			}
			assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("alice"))
			wg.Done()
		}()
		go func() {
			wantAliceOps := map[int64]types.Operation{
				2: op2,
			}
			assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("bob"))
			wg.Done()
		}()
		go func() {
			wantAliceOps := map[int64]types.Operation{
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
			assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetAllTransactions())
			wg.Done()
		}()

		// Wait for all getter operations to complete
		wg.Wait()

		// Assert txByHash
		wantTxByHash := map[string]types.Transaction{
			"tx_hash_1": tx1,
			"tx_hash_2": tx2,
		}
		assert.Equal(t, wantTxByHash, indexerBuffer.txByHash)

		// Assert participants txHashes
		wantTxHashesByParticipant := map[string]set.Set[string]{
			"alice": set.NewSet("tx_hash_1", "tx_hash_2"),
			"bob":   set.NewSet("tx_hash_2"),
			"chuck": set.NewSet("tx_hash_2"),
		}
		assert.Equal(t, wantTxHashesByParticipant, indexerBuffer.txHashesByParticipant)

		// Assert operations
		wantOpByID := map[int64]types.Operation{
			1: op1,
			2: op2,
		}
		assert.Equal(t, wantOpByID, indexerBuffer.opByID)
	})
}

func Test_IndexerBuffer_StateChanges(t *testing.T) {
	t.Run("🟢sequential_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		sc1 := types.StateChange{ID: "sc1"}
		sc2 := types.StateChange{ID: "sc2"}
		sc3 := types.StateChange{ID: "sc3"}

		indexerBuffer.PushStateChanges([]types.StateChange{sc1})
		indexerBuffer.PushStateChanges([]types.StateChange{sc2, sc3})

		allStateChanges := indexerBuffer.GetAllStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})

	t.Run("🟢concurrent_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		sc1 := types.StateChange{ID: "sc1"}
		sc2 := types.StateChange{ID: "sc2"}
		sc3 := types.StateChange{ID: "sc3"}

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()
			indexerBuffer.PushStateChanges([]types.StateChange{sc1})
		}()

		go func() {
			defer wg.Done()
			indexerBuffer.PushStateChanges([]types.StateChange{sc2, sc3})
		}()

		wg.Wait()

		allStateChanges := indexerBuffer.GetAllStateChanges()
		assert.Len(t, allStateChanges, 3)
		assert.ElementsMatch(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})
}
