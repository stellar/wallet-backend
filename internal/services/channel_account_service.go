package services

import (
	"context"
	"fmt"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
)

type ChannelAccountService interface {
	EnsureChannelAccounts(ctx context.Context, number int64) error
}

type channelAccountService struct {
	DB                                 db.ConnectionPool
	HorizonClient                      horizonclient.ClientInterface
	BaseFee                            int64
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	PrivateKeyEncrypter                signingutils.PrivateKeyEncrypter
	EncryptionPassphrase               string
}

var _ ChannelAccountService = (*channelAccountService)(nil)

func (s *channelAccountService) EnsureChannelAccounts(ctx context.Context, number int64) error {
	currentChannelAccountNumber, err := s.ChannelAccountStore.Count(ctx)
	if err != nil {
		return fmt.Errorf("getting the number of channel account already stored: %w", err)
	}

	numOfChannelAccountsToCreate := number - currentChannelAccountNumber
	// The number of channel accounts stored is sufficient.
	if numOfChannelAccountsToCreate <= 0 {
		return nil
	}

	distributionAccountPublicKey, err := s.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return fmt.Errorf("getting distribution account public key: %w", err)
	}

	ops := make([]txnbuild.Operation, 0, numOfChannelAccountsToCreate)
	channelAccountsToInsert := []*store.ChannelAccount{}
	for range numOfChannelAccountsToCreate {
		kp, err := keypair.Random()
		if err != nil {
			return fmt.Errorf("generating random keypair for channel account: %w", err)
		}

		encryptedPrivateKey, err := s.PrivateKeyEncrypter.Encrypt(ctx, kp.Seed(), s.EncryptionPassphrase)
		if err != nil {
			return fmt.Errorf("encrypting channel account private key: %w", err)
		}

		ops = append(ops, &txnbuild.CreateAccount{
			Destination:   kp.Address(),
			Amount:        "1",
			SourceAccount: distributionAccountPublicKey,
		})
		channelAccountsToInsert = append(channelAccountsToInsert, &store.ChannelAccount{
			PublicKey:           kp.Address(),
			EncryptedPrivateKey: encryptedPrivateKey,
		})
	}

	if err = s.submitCreateChannelAccountsOnChainTransaction(ctx, distributionAccountPublicKey, ops); err != nil {
		return fmt.Errorf("submitting create channel accounts on chain transaction: %w", err)
	}

	if err = s.ChannelAccountStore.BatchInsert(ctx, s.DB, channelAccountsToInsert); err != nil {
		return fmt.Errorf("inserting channel accounts: %w", err)
	}

	return nil
}

func (s *channelAccountService) submitCreateChannelAccountsOnChainTransaction(ctx context.Context, distributionAccountPublicKey string, ops []txnbuild.Operation) error {
	distributionAccount, err := s.HorizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: distributionAccountPublicKey})
	if err != nil {
		return fmt.Errorf("getting account detail of channel account: %w", err)
	}

	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount:        &distributionAccount,
			IncrementSequenceNum: true,
			Operations:           ops,
			BaseFee:              s.BaseFee,
			Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		},
	)
	if err != nil {
		return fmt.Errorf("building transaction: %w", err)
	}

	signedTx, err := s.DistributionAccountSignatureClient.SignStellarTransaction(ctx, tx, distributionAccountPublicKey)
	if err != nil {
		return fmt.Errorf("signing transaction: %w", err)
	}

	hash, err := signedTx.HashHex(s.DistributionAccountSignatureClient.NetworkPassphrase())
	if err != nil {
		return fmt.Errorf("getting transaction hash: %w", err)
	}

	_, err = s.HorizonClient.SubmitTransaction(signedTx)
	if err != nil {
		if hError := horizonclient.GetError(err); hError != nil {
			hProblem := hError.Problem
			if hProblem.Type == "https://stellar.org/horizon-errors/timeout" {
				return fmt.Errorf("horizon request timed out while creating a channel account. Transaction hash: %s", hash)
			}

			errString := fmt.Sprintf("Type: %s, Title: %s, Status: %d, Detail: %s, Extras: %v", hProblem.Type, hProblem.Title, hProblem.Status, hProblem.Detail, hProblem.Extras)
			return fmt.Errorf("submitting transaction: %s: %w", errString, err)
		} else {
			return fmt.Errorf("submitting transaction: %w", err)
		}
	}

	return nil
}

type ChannelAccountServiceOptions struct {
	DB                                 db.ConnectionPool
	HorizonClient                      horizonclient.ClientInterface
	BaseFee                            int64
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	PrivateKeyEncrypter                signingutils.PrivateKeyEncrypter
	EncryptionPassphrase               string
}

func (o *ChannelAccountServiceOptions) Validate() error {
	if o.DB == nil {
		return fmt.Errorf("DB cannot be nil")
	}

	if o.HorizonClient == nil {
		return fmt.Errorf("horizon client cannot be nil")
	}

	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	if o.DistributionAccountSignatureClient == nil {
		return fmt.Errorf("distribution account signature client cannot be nil")
	}

	if o.ChannelAccountStore == nil {
		return fmt.Errorf("channel account store cannot be nil")
	}

	if o.PrivateKeyEncrypter == nil {
		return fmt.Errorf("private key encrypter cannot be nil")
	}

	if o.EncryptionPassphrase == "" {
		return fmt.Errorf("encryption passphrase cannot be empty")
	}

	return nil
}

func NewChannelAccountService(opts ChannelAccountServiceOptions) (*channelAccountService, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &channelAccountService{
		DB:                                 opts.DB,
		HorizonClient:                      opts.HorizonClient,
		BaseFee:                            opts.BaseFee,
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountStore:                opts.ChannelAccountStore,
		PrivateKeyEncrypter:                opts.PrivateKeyEncrypter,
		EncryptionPassphrase:               opts.EncryptionPassphrase,
	}, nil
}
