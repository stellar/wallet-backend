package auth

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type MockSignatureVerifier struct {
	mock.Mock
}

var _ SignatureVerifier = (*MockSignatureVerifier)(nil)

func (sv *MockSignatureVerifier) VerifySignature(ctx context.Context, signatureHeaderContent string, reqBody []byte) error {
	args := sv.Called(ctx, signatureHeaderContent, reqBody)
	return args.Error(0)
}
