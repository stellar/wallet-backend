package services

import (
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stretchr/testify/mock"
)

type MockService struct {
	mock.Mock
}

var _ Service = (*MockService)(nil)

func (s *MockService) ProcessPayload(payload tss.Payload) {
	s.Called(payload)
}
