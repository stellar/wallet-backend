package services

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
	"github.com/stellar/wallet-backend/internal/tss/router"
)

const (
	maxRetriesForChannelAccountCreation    = 10
	sleepDelayForChannelAccountCreation    = 10 * time.Second
)

type ChannelAccountService interface {
	EnsureChannelAccounts(ctx context.Context, number int64) error
}

type channelAccountService struct {
	DB                                 db.ConnectionPool
	RPCService                         RPCService
	BaseFee                            int64
	Router                             router.Router
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
	err := waitForRPCServiceHealth(ctx, s.RPCService)
	if err != nil {
		return fmt.Errorf("rpc service did not become healthy: %w", err)
	}

	accountSeq, err := s.RPCService.GetAccountLedgerSequence(distributionAccountPublicKey)
	if err != nil {
		return fmt.Errorf("getting ledger sequence for distribution account public key: %s: %w", distributionAccountPublicKey, err)
	}

	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: distributionAccountPublicKey,
				Sequence:  accountSeq,
			},
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

	signedTxXDR, err := signedTx.Base64()
	if err != nil {
		return fmt.Errorf("getting transaction envelope: %w", err)
	}

	err = s.submitTransaction(ctx, hash, signedTxXDR)
	if err != nil {
		return fmt.Errorf("submitting channel account transaction to rpc service: %w", err)
	}

	err = s.getTransactionStatus(ctx, hash)
	if err != nil {
		return fmt.Errorf("getting transaction status: %w", err)
	}

	return nil
}

func waitForRPCServiceHealth(ctx context.Context, rpcService RPCService) error {
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	heartbeat := make(chan entities.RPCGetHealthResult, 1)
	go trackRPCServiceHealth(cancelCtx, heartbeat, nil, rpcService)

	for {
		select {
		case <-heartbeat:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		}
	}
}

func (s *channelAccountService) submitTransaction(_ context.Context, hash string, signedTxXDR string) error {
	for range maxRetriesForChannelAccountCreation {
		result, err := s.RPCService.SendTransaction(signedTxXDR)
		if err != nil {
			return fmt.Errorf("sending transaction: %s: %w", hash, err)
		}

		switch result.Status {
		case entities.PendingStatus:
			return nil
		case entities.ErrorStatus:
			return fmt.Errorf("transaction failed %s: %s", result.ErrorResultXDR, hash)
		case entities.TryAgainLaterStatus:
			time.Sleep(sleepDelayForChannelAccountCreation)
			continue
		}
	
	}

	return fmt.Errorf("transaction did not complete after %d attempts", maxRetriesForChannelAccountCreation)
}

func (s *channelAccountService) getTransactionStatus(_ context.Context, hash string) error {
	for range maxRetriesForChannelAccountCreation {
		txResult, err := s.RPCService.GetTransaction(hash)
		if err != nil {
			return fmt.Errorf("getting transaction status response: %w", err)
		}

		switch txResult.Status {
		case entities.NotFoundStatus:
			time.Sleep(sleepDelayForChannelAccountCreation)
			continue
		case entities.SuccessStatus:
			return nil
		case entities.FailedStatus:
			return fmt.Errorf("transaction failed: %s: %s: %s", hash, txResult.Status, txResult.ErrorResultXDR)
		}
	}

	return fmt.Errorf("failed to get transaction status after %d attempts", maxRetriesForChannelAccountCreation)
}

type ChannelAccountServiceOptions struct {
	DB                                 db.ConnectionPool
	RPCService                         RPCService
	BaseFee                            int64
	Router                             router.Router
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	PrivateKeyEncrypter                signingutils.PrivateKeyEncrypter
	EncryptionPassphrase               string
}

func (o *ChannelAccountServiceOptions) Validate() error {
	if o.DB == nil {
		return fmt.Errorf("DB cannot be nil")
	}

	if o.RPCService == nil {
		return fmt.Errorf("rpc client cannot be nil")
	}

	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	if o.Router == nil {
		return fmt.Errorf("router cannot be nil")
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
		RPCService:                         opts.RPCService,
		BaseFee:                            opts.BaseFee,
		Router:                             opts.Router,
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountStore:                opts.ChannelAccountStore,
		PrivateKeyEncrypter:                opts.PrivateKeyEncrypter,
		EncryptionPassphrase:               opts.EncryptionPassphrase,
	}, nil
}
