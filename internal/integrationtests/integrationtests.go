package integrationtests

import (
	"context"
	"fmt"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/services"
)

type IntegrationTestsOptions struct {
	BaseFee              int64
	NetworkPassphrase    string
	RPCService           services.RPCService
	ClientAuthPrivateKey string
}

func (o *IntegrationTestsOptions) Validate() error {
	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	if o.ClientAuthPrivateKey == "" {
		return fmt.Errorf("client auth private key cannot be empty")
	}

	if o.NetworkPassphrase == "" {
		return fmt.Errorf("network passphrase cannot be empty")
	}

	if o.RPCService == nil {
		return fmt.Errorf("rpc client cannot be nil")
	}

	return nil
}

type IntegrationTests struct {
	BaseFee              int64
	NetworkPassphrase    string
	RPCService           services.RPCService
	ClientAuthPrivateKey string
}

func (i *IntegrationTests) Run(ctx context.Context) error {
	log.Ctx(ctx).Info("TODO: run integration tests")
	return nil
}

func NewIntegrationTests(ctx context.Context, opts IntegrationTestsOptions) (*IntegrationTests, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validating integration tests options: %w", err)
	}

	go opts.RPCService.TrackRPCServiceHealth(ctx)

	return &IntegrationTests{
		BaseFee:              opts.BaseFee,
		NetworkPassphrase:    opts.NetworkPassphrase,
		RPCService:           opts.RPCService,
		ClientAuthPrivateKey: opts.ClientAuthPrivateKey,
	}, nil
}
