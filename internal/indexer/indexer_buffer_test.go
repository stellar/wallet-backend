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
		indexerBuffer.PushParticipantTransaction("alice", tx1)
		indexerBuffer.PushParticipantTransaction("alice", tx2)
		indexerBuffer.PushParticipantTransaction("bob", tx2)
		indexerBuffer.PushParticipantTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally

		// Assert participants txHashes
		wantTxHashesByParticipant := map[string]set.Set[string]{
			"alice": set.NewSet("tx_hash_1", "tx_hash_2"),
			"bob":   set.NewSet("tx_hash_2"),
		}
		assert.Equal(t, wantTxHashesByParticipant, indexerBuffer.txHashesByParticipant)

		// Assert GetParticipantTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))

		// Assert txByHash
		wantTxByHash := map[string]types.Transaction{
			"tx_hash_1": tx1,
			"tx_hash_2": tx2,
		}
		assert.Equal(t, wantTxByHash, indexerBuffer.txByHash)

		// Assert Participants
		assert.Equal(t, set.NewSet("alice", "bob"), indexerBuffer.Participants)

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetAllTransactions())
	})

	t.Run("🟢concurrent_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

		// Concurrent push operations
		wg := sync.WaitGroup{}
		wg.Add(4)
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
		wg.Wait()

		// Concurrent getter operations
		wg = sync.WaitGroup{}
		wg.Add(5)

		// GetParticipantTransactions
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))
			wg.Done()
		}()

		// Assert GetNumberOfTransactions
		go func() {
			assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())
			wg.Done()
		}()

		// Assert Participants
		go func() {
			assert.Equal(t, set.NewSet("alice", "bob"), indexerBuffer.Participants)
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
		}
		assert.Equal(t, wantTxHashesByParticipant, indexerBuffer.txHashesByParticipant)
	})
}
