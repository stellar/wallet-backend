package services

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
)

const (
	maxRetriesForChannelAccountCreation = 50
	sleepDelayForChannelAccountCreation = 10 * time.Second
	rpcHealthCheckTimeout = 5 * time.Minute // We want a slightly longer timeout to give time to rpc to catch up to the tip when we start wallet-backend
)

type ChannelAccountService interface {
	EnsureChannelAccounts(ctx context.Context, number int64) error
}

type channelAccountService struct {
	DB                                 db.ConnectionPool
	RPCService                         RPCService
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
	log.Ctx(ctx).Infof("🔍 Channel accounts amounts: {desired:%d, current:%d, pendingCreation:%d}", number, currentChannelAccountNumber, int(math.Max(float64(numOfChannelAccountsToCreate), 0)))
	if numOfChannelAccountsToCreate <= 0 {
		log.Ctx(ctx).Infof("✅ No channel accounts to create")
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

		log.Ctx(ctx).Infof("⏳ Creating sponsored Stellar channel account with address: %s", chAccKP.Address())
	}

	if err = s.submitCreateChannelAccountsOnChainTransaction(ctx, distributionAccountPublicKey, ops); err != nil {
		return fmt.Errorf("submitting create channel accounts on chain transaction: %w", err)
	}
	log.Ctx(ctx).Infof("🎉 Successfully created %d sponsored channel accounts", len(chAccKPs))

	if err = s.ChannelAccountStore.BatchInsert(ctx, s.DB, channelAccountsToInsert); err != nil {
		return fmt.Errorf("inserting channel accounts: %w", err)
	}
	log.Ctx(ctx).Infof("✅ Successfully stored %d channel accounts into the store", len(channelAccountsToInsert))

	return nil
}

func (s *channelAccountService) submitCreateChannelAccountsOnChainTransaction(ctx context.Context, distributionAccountPublicKey string, ops []txnbuild.Operation) error {
	rpcHeartbeatChannel := s.RPCService.GetHeartbeatChannel()
	log.Ctx(ctx).Infof("⏳ Waiting for RPC service to become healthy")
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for rpc service to become healthy: %w", ctx.Err())
	// The channel account creation goroutine will wait in the background for the rpc service to become healthy on startup.
	// This lets the API server startup so that users can start interacting with the API which does not depend on RPC, instead of waiting till it becomes healthy.
	case <-rpcHeartbeatChannel:
		accountSeq, err := s.RPCService.GetAccountLedgerSequence(distributionAccountPublicKey)
		log.Ctx(ctx).Infof("👍 RPC service is healthy")
		log.Ctx(ctx).Infof("🚧 Submitting channel account transaction to rpc service")
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

		err = s.waitForTransactionConfirmation(ctx, hash)
		if err != nil {
			return fmt.Errorf("getting transaction status: %w", err)
		}

		return nil
	}
}

func (s *channelAccountService) submitTransaction(_ context.Context, hash string, signedTxXDR string) error {
	for range maxRetriesForChannelAccountCreation {
		result, err := s.RPCService.SendTransaction(signedTxXDR)
		if err != nil {
			return fmt.Errorf("sending transaction with hash %q: %w", hash, err)
		}

		//exhaustive:ignore
		switch result.Status {
		case entities.PendingStatus:
			return nil
		case entities.ErrorStatus:
			return fmt.Errorf("transaction with hash %q failed with result %s", hash, result.ErrorResultXDR)
		case entities.TryAgainLaterStatus:
			time.Sleep(sleepDelayForChannelAccountCreation)
			continue
		}
	}

	return fmt.Errorf("transaction did not complete after %d attempts", maxRetriesForChannelAccountCreation)
}

func (s *channelAccountService) waitForTransactionConfirmation(_ context.Context, hash string) error {
	for range maxRetriesForChannelAccountCreation {
		txResult, err := s.RPCService.GetTransaction(hash)
		if err != nil {
			return fmt.Errorf("getting transaction status response: %w", err)
		}

		//exhaustive:ignore
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

func NewChannelAccountService(ctx context.Context, opts ChannelAccountServiceOptions) (*channelAccountService, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	go opts.RPCService.TrackRPCServiceHealth(ctx)

	return &channelAccountService{
		DB:                                 opts.DB,
		RPCService:                         opts.RPCService,
		BaseFee:                            opts.BaseFee,
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountStore:                opts.ChannelAccountStore,
		PrivateKeyEncrypter:                opts.PrivateKeyEncrypter,
		EncryptionPassphrase:               opts.EncryptionPassphrase,
	}, nil
}
