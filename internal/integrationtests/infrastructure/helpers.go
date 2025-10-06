// Package infrastructure provides helper utilities for integration testing
package infrastructure

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/pkg/utils"
)

// ConvertOperationsToBase64XDR converts a slice of operations to their base64 XDR representations.
func ConvertOperationsToBase64XDR(operations []txnbuild.Operation) ([]string, error) {
	b64OpsXDRs := make([]string, len(operations))
	for i, op := range operations {
		opXDR, err := op.BuildXDR()
		if err != nil {
			return nil, fmt.Errorf("building operation XDR: %w", err)
		}
		b64OpsXDRs[i], err = utils.OperationXDRToBase64(opXDR)
		if err != nil {
			return nil, fmt.Errorf("encoding operation XDR to base64: %w", err)
		}
	}
	return b64OpsXDRs, nil
}

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

func WaitForTransactionConfirmation(ctx context.Context, rpcService services.RPCService, hash string, retryOptions ...retry.Option) (txResult entities.RPCGetTransactionResult, err error) {
	attemptsCount := 0
	outerErr := retry.Do(
		func() error {
			attemptsCount++
			log.Ctx(ctx).Infof("🔁 attemptsCount: %d", attemptsCount)
			txResult, err = rpcService.GetTransaction(hash)
			if err != nil {
				return fmt.Errorf("getting transaction with hash %q: %w", hash, err)
			}

			switch txResult.Status {
			case entities.NotFoundStatus:
				return fmt.Errorf("transaction not found")
			case entities.SuccessStatus, entities.FailedStatus:
				return nil
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
		return entities.RPCGetTransactionResult{}, fmt.Errorf("failed to get transaction status after %d attempts: %w", attemptsCount, outerErr)
	}
	return txResult, nil
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

type Set[T comparable] struct {
	set map[T]struct{}
}

func NewSet[T comparable](values ...T) *Set[T] {
	set := &Set[T]{set: make(map[T]struct{})}
	for _, value := range values {
		set.Add(value)
	}
	return set
}

func (s *Set[T]) Add(value T) {
	if value != *new(T) {
		s.set[value] = struct{}{}
	}
}

func (s *Set[T]) Slice() []T {
	slice := make([]T, 0, len(s.set))
	for value := range s.set {
		slice = append(slice, value)
	}
	return slice
}

// RenderResult renders a result string for a use case.
func RenderResult(useCase *UseCase) string {
	status := useCase.GetTransactionResult.Status
	var statusEmoji string
	switch status {
	case entities.SuccessStatus:
		statusEmoji = "✅"
	case entities.FailedStatus:
		statusEmoji = "❌"
	case entities.NotFoundStatus:
		statusEmoji = "⏳"
	default:
		statusEmoji = "⁉️"
	}
	statusText := fmt.Sprintf("%s %s", statusEmoji, status)

	var builder strings.Builder

	builder.WriteString(statusText)
	builder.WriteString(fmt.Sprintf(" {Use Case: %s", useCase.name))
	builder.WriteString(fmt.Sprintf(", Category: %s", useCase.category))
	builder.WriteString(fmt.Sprintf(", Hash: %s", useCase.SendTransactionResult.Hash))
	if status != entities.SuccessStatus {
		txResult := useCase.GetTransactionResult
		builder.WriteString(fmt.Sprintf("ResultXDR: %+v, ErrorResultXDR: %+v, ResultMetaXDR: %+v", txResult.ResultXDR, txResult.ErrorResultXDR, txResult.ResultMetaXDR))
	}
	builder.WriteString("}")

	return builder.String()
}
