package router

import (
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/tss"
)

type MockRouter struct {
	mock.Mock
}

var _ Router = (*MockRouter)(nil)

func (r *MockRouter) Route(payload tss.Payload) error {
	args := r.Called(payload)
	return args.Error(0)
}
