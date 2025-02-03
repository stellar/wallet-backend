package services

import (
	"context"
	"fmt"

	"errors"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type AccountService interface {
	// RegisterAccount registers an externally created Stellar account to be sponsored, and tracked by ingestion
	RegisterAccount(ctx context.Context, address string) error
	// DeregisterAccount deregisters a Stellar account, no longer sponsoring its transactions, nor tracking it on ingestion
	DeregisterAccount(ctx context.Context, address string) error
}

var _ AccountService = (*accountService)(nil)

type accountService struct {
	models *data.Models
	metricsService *metrics.MetricsService
}

func NewAccountService(models *data.Models, metricsService *metrics.MetricsService) (*accountService, error) {
	if models == nil {
		return nil, errors.New("models cannot be nil")
	}

	return &accountService{
		models: models,
		metricsService: metricsService,
	}, nil
}

func (s *accountService) RegisterAccount(ctx context.Context, address string) error {
	err := s.models.Account.Insert(ctx, address)
	if err != nil {
		return fmt.Errorf("registering account %s: %w", address, err)
	}
	s.metricsService.SetNumAccountsRegistered(address)
	return nil
}

func (s *accountService) DeregisterAccount(ctx context.Context, address string) error {
	err := s.models.Account.Delete(ctx, address)
	if err != nil {
		return fmt.Errorf("deregistering account %s: %w", address, err)
	}
	s.metricsService.SetNumAccountsDeregistered(address)
	return nil
}
