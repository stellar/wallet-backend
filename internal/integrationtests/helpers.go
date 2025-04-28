package integrationtests

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
)

// WaitForRPCHealthAndRun waits for the RPC service to become healthy and then runs the given function.
func WaitForRPCHealthAndRun(ctx context.Context, rpcService services.RPCService, timeout time.Duration, onReady func() error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	log.Ctx(ctx).Info("⏳ Waiting for RPC service to become healthy...")
	rpcHeartbeatChannel := rpcService.GetHeartbeatChannel()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer signal.Stop(signalChan)

	select {
	case <-ctx.Done():
		return fmt.Errorf("context canceled while waiting for RPC service to become healthy: %w", ctx.Err())

	case sig := <-signalChan:
		return fmt.Errorf("received signal %s while waiting for RPC service to become healthy", sig)

	case <-rpcHeartbeatChannel:
		log.Ctx(ctx).Info("👍 RPC service is healthy")
		if onReady != nil {
			if err := onReady(); err != nil {
				return fmt.Errorf("executing onReady after RPC became healthy: %w", err)
			}
		}
		return nil
	}
}

func WaitForTransactionConfirmation(ctx context.Context, rpcService services.RPCService, hash string, retryOptions ...retry.Option) error {
	attemptsCount := 0
	outerErr := retry.Do(
		func() error {
			attemptsCount++
			log.Ctx(ctx).Infof("🔁 attemptsCount: %d", attemptsCount)
			txResult, err := rpcService.GetTransaction(hash)
			if err != nil {
				return fmt.Errorf("getting transaction with hash %q: %w", hash, err)
			}

			switch txResult.Status {
			case entities.NotFoundStatus:
				return fmt.Errorf("transaction not found")
			case entities.SuccessStatus:
				return nil
			case entities.FailedStatus:
				err = fmt.Errorf("transaction with hash %q failed with status %s and errorResultXdr %s", hash, txResult.Status, txResult.ErrorResultXDR)
				return retry.Unrecoverable(err)
			default:
				return fmt.Errorf("unexpected transaction status: %s", txResult.Status)
			}
		},
		append(
			retryOptions,
			retry.Context(ctx),
			retry.LastErrorOnly(true),
		)...,
	)

	if outerErr != nil {
		return fmt.Errorf("failed to get transaction status after %d attempts: %w", attemptsCount, outerErr)
	}
	return nil
}

func SCAccountID(address string) (xdr.ScAddress, error) {
	accountID, err := xdr.AddressToAccountId(address)
	if err != nil {
		return xdr.ScAddress{}, fmt.Errorf("marshalling from address: %w", err)
	}

	return xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &accountID,
	}, nil
}
