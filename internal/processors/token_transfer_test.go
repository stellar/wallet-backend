package processors

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testFile          = "testdata/test-ledger.txt"
	networkPassphrase = "Public Global Stellar Network ; September 2015"
)

func TestTokenTransferProcessor_ProcessTransaction(t *testing.T) {
	ledgers, err := readTestLedgers(t)
	require.NoError(t, err)

	t.Run("extracts all state changes from the ledgers", func(t *testing.T) {
		processor := NewTokenTransferProcessor(nil, networkPassphrase)

		for _, ledger := range ledgers {
			// Create transaction reader from ledger
			txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, ledger)
			require.NoError(t, err)
			defer txReader.Close()

			// Collect all state changes from all transactions
			var allStateChanges []types.StateChange

			for {
				tx, err := txReader.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)

				stateChanges, err := processor.ProcessTransaction(context.Background(), tx)
				require.NoError(t, err)

				allStateChanges = append(allStateChanges, stateChanges...)
			}

			require.NotEmpty(t, allStateChanges)
			// For each transfer event, we generate 2 state changes. Our test ledger has 8 "Transfer" events and 19 "Fee" events.
			// Hence the total state changes generated are 35.
			assert.Equal(t, 2*8+19, len(allStateChanges))

			// Check that the state changes are valid
			for _, stateChange := range allStateChanges {
				assert.NotEmpty(t, stateChange.AccountID)
				assert.NotEmpty(t, stateChange.Amount)
				assert.Equal(t, int64(26154623), stateChange.LedgerNumber)
			}

			// Verify that specific state changes exist (order may vary)
			foundNativeTransfer := false
			foundMFNTransfer := false

			for _, sc := range allStateChanges {
				if sc.AccountID == "GANIJUT5I2J4QJHR6CIXXC5O5KSIMUNM47HIQYDAN5RYE7UM5WSHU4GV" &&
					sc.Token.String == "native" &&
					sc.Amount.String == "100" {
					foundNativeTransfer = true
				}

				if sc.AccountID == "GBJVYTCIAEOINWPDE7SZPI4JJ3L4F53VBJ7R3F27T2BYZALLQCKG3Z5V" &&
					sc.Token.String == "MFN:GC55P5MTVPOPPY7NBBS5RPRUF5K3667ZQ2GN4J5GGE6AZVLPC72S5K46" &&
					sc.Amount.String == "11250000" {
					foundMFNTransfer = true
				}
			}

			assert.True(t, foundNativeTransfer, "Expected native transfer not found")
			assert.True(t, foundMFNTransfer, "Expected MFN transfer not found")
		}
	})
}

// Helper function to read a single line from the input file
func readTestLedgers(t *testing.T) ([]xdr.LedgerCloseMeta, error) {
	file, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("failed to open test file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	ledgers := make([]xdr.LedgerCloseMeta, 0)
	for scanner.Scan() {
		var ledger xdr.LedgerCloseMeta
		if err := xdr.SafeUnmarshalBase64(scanner.Text(), &ledger); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ledger: %w", err)
		}
		ledgers = append(ledgers, ledger)
	}
	return ledgers, nil
}
