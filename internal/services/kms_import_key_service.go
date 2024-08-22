package services

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
	"github.com/stellar/wallet-backend/internal/signing/awskms"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

var (
	ErrInvalidPrivateKeyProvided   = errors.New("invalid private key provided")
	ErrMismatchDistributionAccount = errors.New("mismatch distribution account")
)

type KMSImportService interface {
	ImportDistributionAccountKey(ctx context.Context, distributionAccountSeed string) error
}

type kmsImportService struct {
	client                       kmsiface.KMSAPI
	kmsKeyARN                    string
	keypairStore                 store.KeypairStore
	distributionAccountPublicKey string
}

var _ KMSImportService = (*kmsImportService)(nil)

func (s *kmsImportService) ImportDistributionAccountKey(ctx context.Context, distributionAccountSeed string) error {
	if !strkey.IsValidEd25519SecretSeed(distributionAccountSeed) {
		return ErrInvalidPrivateKeyProvided
	}

	kp, err := keypair.ParseFull(distributionAccountSeed)
	if err != nil {
		return fmt.Errorf("parsing distribution private key: %s", err)
	}

	if kp.Address() != s.distributionAccountPublicKey {
		return ErrMismatchDistributionAccount
	}

	input := kms.EncryptInput{
		EncryptionContext: awskms.GetPrivateKeyEncryptionContext(kp.Address()),
		KeyId:             aws.String(s.kmsKeyARN),
		Plaintext:         []byte(distributionAccountSeed),
	}
	output, err := s.client.Encrypt(&input)
	if err != nil {
		return fmt.Errorf("encrypting distribution account private key: %w", err)
	}

	err = s.keypairStore.Insert(ctx, kp.Address(), output.CiphertextBlob)
	if err != nil {
		if errors.Is(err, store.ErrPublicKeyAlreadyExists) {
			return err
		}
		return fmt.Errorf("storing distribution account encrypted private key: %w", err)
	}

	return nil
}

func NewKMSImportService(client kmsiface.KMSAPI, kmsKeyARN string, keypairStore store.KeypairStore, distributionAccountPublicKey string) (*kmsImportService, error) {
	if client == nil {
		return nil, fmt.Errorf("kms cannot be nil")
	}

	kmsKeyARN = strings.TrimSpace(kmsKeyARN)
	if kmsKeyARN == "" {
		return nil, fmt.Errorf("aws key arn cannot be empty")
	}

	if keypairStore == nil {
		return nil, fmt.Errorf("keypair store cannot be nil")
	}

	if !strkey.IsValidEd25519PublicKey(distributionAccountPublicKey) {
		return nil, fmt.Errorf("invalid distribution account public key provided")
	}

	return &kmsImportService{
		kmsKeyARN:                    kmsKeyARN,
		client:                       client,
		keypairStore:                 keypairStore,
		distributionAccountPublicKey: distributionAccountPublicKey,
	}, nil
}
