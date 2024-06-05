package servicesmocks

import (
	"context"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stretchr/testify/mock"
)

type AccountSponsorshipServiceMock struct {
	mock.Mock
}

var _ services.AccountSponsorshipService = (*AccountSponsorshipServiceMock)(nil)

func (s *AccountSponsorshipServiceMock) SponsorAccountCreationTransaction(ctx context.Context, accountToSponsor string, signers []entities.Signer, assets []entities.Asset) (string, string, error) {
	args := s.Called(ctx, accountToSponsor, signers, assets)
	return args.String(0), args.String(1), args.Error(2)
}
