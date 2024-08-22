package awskms

import (
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"
	"github.com/stretchr/testify/mock"
)

type KMSMock struct {
	kmsiface.KMSAPI
	mock.Mock
}

func (k *KMSMock) Encrypt(input *kms.EncryptInput) (*kms.EncryptOutput, error) {
	args := k.Called(input)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*kms.EncryptOutput), args.Error(1)
}

func (k *KMSMock) Decrypt(input *kms.DecryptInput) (*kms.DecryptOutput, error) {
	args := k.Called(input)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*kms.DecryptOutput), args.Error(1)
}
