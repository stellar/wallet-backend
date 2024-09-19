package router

import (
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stretchr/testify/mock"
)

type MockRouter struct {
	mock.Mock
}

var _ Router = (*MockRouter)(nil)

func (r *MockRouter) Route(payload tss.Payload) {
	r.Called(payload)
}
