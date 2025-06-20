package indexer

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func Test_IngestionBuffer_PushParticipantTransaction_and_Getters(t *testing.T) {
	t.Run("🟢sequential_operations", func(t *testing.T) {
		ingestionBuffer := NewIngestionBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
		ingestionBuffer.PushParticipantTransaction("alice", tx1)
		ingestionBuffer.PushParticipantTransaction("alice", tx2)
		ingestionBuffer.PushParticipantTransaction("bob", tx2)
		ingestionBuffer.PushParticipantTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally

		// GetParticipantTransactionHashes
		assert.ElementsMatch(t, []string{"tx_hash_1", "tx_hash_2"}, ingestionBuffer.GetParticipantTransactionHashes("alice").ToSlice())
		assert.ElementsMatch(t, []string{"tx_hash_2"}, ingestionBuffer.GetParticipantTransactionHashes("bob").ToSlice())

		// GetParticipantTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, ingestionBuffer.GetParticipantTransactions("alice"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, ingestionBuffer.GetParticipantTransactions("bob"))

		// GetTransactionParticipants
		assert.ElementsMatch(t, []string{"alice"}, ingestionBuffer.GetTransactionParticipants("tx_hash_1").ToSlice())
		assert.ElementsMatch(t, []string{"alice", "bob"}, ingestionBuffer.GetTransactionParticipants("tx_hash_2").ToSlice())

		// GetTransaction
		assert.Equal(t, tx1, ingestionBuffer.GetTransaction("tx_hash_1"))
		assert.Equal(t, tx2, ingestionBuffer.GetTransaction("tx_hash_2"))

		// Get Participants
		assert.ElementsMatch(t, []string{"alice", "bob"}, ingestionBuffer.Participants.ToSlice())
	})

	t.Run("🟢concurrent_operations", func(t *testing.T) {
		ingestionBuffer := NewIngestionBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

		// Concurrent push operations
		wg := sync.WaitGroup{}
		wg.Add(4)
		go func() {
			ingestionBuffer.PushParticipantTransaction("alice", tx1)
			wg.Done()
		}()
		go func() {
			ingestionBuffer.PushParticipantTransaction("alice", tx2)
			wg.Done()
		}()
		go func() {
			ingestionBuffer.PushParticipantTransaction("bob", tx2)
			wg.Done()
		}()
		go func() {
			ingestionBuffer.PushParticipantTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally
			wg.Done()
		}()
		wg.Wait()

		// Concurrent getter operations
		wg = sync.WaitGroup{}
		wg.Add(8)

		// GetParticipantTransactionHashes
		go func() {
			assert.ElementsMatch(t, []string{"tx_hash_1", "tx_hash_2"}, ingestionBuffer.GetParticipantTransactionHashes("alice").ToSlice())
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []string{"tx_hash_2"}, ingestionBuffer.GetParticipantTransactionHashes("bob").ToSlice())
			wg.Done()
		}()

		// GetParticipantTransactions
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, ingestionBuffer.GetParticipantTransactions("alice"))
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx2}, ingestionBuffer.GetParticipantTransactions("bob"))
			wg.Done()
		}()

		// GetTransactionParticipants
		go func() {
			assert.ElementsMatch(t, []string{"alice"}, ingestionBuffer.GetTransactionParticipants("tx_hash_1").ToSlice())
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []string{"alice", "bob"}, ingestionBuffer.GetTransactionParticipants("tx_hash_2").ToSlice())
			wg.Done()
		}()

		// GetTransaction
		go func() {
			assert.Equal(t, tx1, ingestionBuffer.GetTransaction("tx_hash_1"))
			wg.Done()
		}()
		go func() {
			assert.Equal(t, tx2, ingestionBuffer.GetTransaction("tx_hash_2"))
			wg.Done()
		}()

		// Wait for all getter operations to complete
		wg.Wait()

		// Final verification of Participants set
		assert.ElementsMatch(t, []string{"alice", "bob"}, ingestionBuffer.Participants.ToSlice())
	})
}
