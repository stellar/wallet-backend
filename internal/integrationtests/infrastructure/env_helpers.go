package infrastructure

import (
	"context"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
)

// WaitForLedgers waits for a number of ledgers to close (approximated by sleep)
func (e *TestEnvironment) WaitForLedgers(ctx context.Context, ledgers int) {
	// Assume 1 second per ledger for standalone + buffer
	duration := time.Duration(ledgers) * 2 * time.Second
	log.Ctx(ctx).Infof("⏳ Waiting for %d ledgers (%s)...", ledgers, duration)
	time.Sleep(duration)
}
