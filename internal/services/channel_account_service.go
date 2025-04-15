package services

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/avast/retry-go/v4"
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
	// MaximumCreateAccountOperationsPerStellarTx is the max number of sponsored accounts we can create in one transaction
	// due to the signature limit.
	MaximumCreateAccountOperationsPerStellarTx = 19
	maxRetriesForChannelAccountCreation        = 50
	sleepDelayForChannelAccountCreation        = 10 * time.Second
	rpcHealthCheckTimeout                      = 5 * time.Minute // We want a slightly longer timeout to give time to rpc to catch up to the tip when we start wallet-backend
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

	err = s.createChannelAccounts(ctx, numOfChannelAccountsToCreate)
	if err != nil {
		return fmt.Errorf("attempting to create %d channel accounts: %w", numOfChannelAccountsToCreate, err)
	}

	return nil
}

// createChannelAccounts creates the channel accounts on the Stellar network.
func (s *channelAccountService) createChannelAccounts(ctx context.Context, numOfChannelAccountsToCreate int64) error {
	chAccKPs := make([]*keypair.Full, 0, numOfChannelAccountsToCreate)
	channelAccountsToInsert := make([]*store.ChannelAccount, 0, numOfChannelAccountsToCreate)
	for range numOfChannelAccountsToCreate {
		chAccKP, err := keypair.Random()
		if err != nil {
			return fmt.Errorf("generating random keypair for channel account: %w", err)
		}
		chAccKPs = append(chAccKPs, chAccKP)

		encryptedPrivateKey, err := s.PrivateKeyEncrypter.Encrypt(ctx, chAccKP.Seed(), s.EncryptionPassphrase)
		if err != nil {
			return fmt.Errorf("encrypting channel account private key: %w", err)
		}

		channelAccountsToInsert = append(channelAccountsToInsert, &store.ChannelAccount{
			PublicKey:           chAccKP.Address(),
			EncryptedPrivateKey: encryptedPrivateKey,
		})

		log.Ctx(ctx).Infof("⏳ Creating sponsored Stellar channel account with address: %s", chAccKP.Address())
	}

	ops, err := s.prepareAccountCreationOps(ctx, chAccKPs)
	if err != nil {
		return fmt.Errorf("preparing operations to insert channel accounts: %w", err)
	}

	distributionAccountPublicKey, err := s.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return fmt.Errorf("getting distribution account public key: %w", err)
	}
	hash, signedTxXDR, err := s.buildAndSignTransaction(ctx, distributionAccountPublicKey, ops, chAccKPs...)
	if err != nil {
		return fmt.Errorf("building and signing transaction: %w", err)
	}

	if err = s.submitTransactionAndWaitForConfirmation(ctx, hash, signedTxXDR); err != nil {
		return fmt.Errorf("submitting create channel accounts on chain transaction: %w", err)
	}
	log.Ctx(ctx).Infof("🎉 Successfully created %d sponsored channel accounts", len(chAccKPs))

	if err = s.ChannelAccountStore.BatchInsert(ctx, s.DB, channelAccountsToInsert); err != nil {
		return fmt.Errorf("inserting channel accounts: %w", err)
	}
	log.Ctx(ctx).Infof("✅ Successfully stored %d channel accounts into the store", len(channelAccountsToInsert))

	return nil
}

// prepareAccountCreationOps prepares the operations to create the channel accounts using the provided keypairs and the
// sponsored reserves feature.
func (s *channelAccountService) prepareAccountCreationOps(_ context.Context, chAccKP []*keypair.Full) ([]txnbuild.Operation, error) {
	if len(chAccKP) > MaximumCreateAccountOperationsPerStellarTx {
		return nil, fmt.Errorf("number of channel accounts to create is greater than the maximum allowed in one transaction (%d)", MaximumCreateAccountOperationsPerStellarTx)
	}

	ops := make([]txnbuild.Operation, 0, len(chAccKP))
	for _, kp := range chAccKP {
		ops = append(ops,
			// add sponsor operations for this account
			&txnbuild.BeginSponsoringFutureReserves{
				SponsoredID: kp.Address(),
			},
			&txnbuild.CreateAccount{
				Destination: kp.Address(),
				Amount:      "0",
			},
			&txnbuild.EndSponsoringFutureReserves{
				SourceAccount: kp.Address(),
			})
	}
	return ops, nil
}

// submitTransactionAndWaitForConfirmation submits a transaction and waits for it to be confirmed.
// It returns an error if the transaction fails to be submitted or if it fails to be confirmed.
func (s *channelAccountService) submitTransactionAndWaitForConfirmation(ctx context.Context, hash, signedTxXDR string) error {
	rpcHeartbeatChannel := s.RPCService.GetHeartbeatChannel()
	log.Ctx(ctx).Infof("⏳ Waiting for RPC service to become healthy")
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for rpc service to become healthy: %w", ctx.Err())
	// The channel account creation goroutine will wait in the background for the rpc service to become healthy on startup.
	// This lets the API server startup so that users can start interacting with the API which does not depend on RPC, instead of waiting till it becomes healthy.
	case <-rpcHeartbeatChannel:
		log.Ctx(ctx).Infof("👍 RPC service is healthy")
		log.Ctx(ctx).Infof("🚧 Submitting channel account transaction to rpc service")
		retryOptions := []retry.Option{retry.Attempts(maxRetriesForChannelAccountCreation), retry.Delay(sleepDelayForChannelAccountCreation)}
		err := s.submitTransactionWithRetry(ctx, hash, signedTxXDR, retryOptions...)
		if err != nil {
			return fmt.Errorf("a submitting channel account transaction to the RPC service: %w", err)
		}

		log.Ctx(ctx).Infof("🚧 Successfully submitted channel account transaction to rpc service, waiting for confirmation")
		err = s.waitForTransactionConfirmation(ctx, hash, retryOptions...)
		if err != nil {
			return fmt.Errorf("waiting for transaction confirmation: %w", err)
		}

		return nil
	}
}

// buildAndSignTransaction builds a transaction with the provided operations and signs it with the provided keypairs.
func (s *channelAccountService) buildAndSignTransaction(ctx context.Context, distributionAccountPublicKey string, ops []txnbuild.Operation, chAccKPs ...*keypair.Full) (hash string, signedTxXDR string, err error) {
	var accountSeq int64
	accountSeq, err = s.RPCService.GetAccountLedgerSequence(distributionAccountPublicKey)
	if err != nil {
		return "", "", fmt.Errorf("getting ledger sequence for distribution account public key: %s: %w", distributionAccountPublicKey, err)
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
		return "", "", fmt.Errorf("building transaction: %w", err)
	}

	signedTx, err := s.DistributionAccountSignatureClient.SignStellarTransaction(ctx, tx, distributionAccountPublicKey)
	if err != nil {
		return "", "", fmt.Errorf("signing transaction with Distribution Account Signature Client: %w", err)
	}
	if len(chAccKPs) > 0 {
		signedTx, err = signedTx.Sign(s.DistributionAccountSignatureClient.NetworkPassphrase(), chAccKPs...)
		if err != nil {
			return "", "", fmt.Errorf("signing transaction with Channel Account keypairs: %w", err)
		}
	}

	hash, err = signedTx.HashHex(s.DistributionAccountSignatureClient.NetworkPassphrase())
	if err != nil {
		return "", "", fmt.Errorf("getting transaction hash: %w", err)
	}

	signedTxXDR, err = signedTx.Base64()
	if err != nil {
		return "", "", fmt.Errorf("getting transaction envelope: %w", err)
	}

	return hash, signedTxXDR, nil
}

func (s *channelAccountService) submitTransactionWithRetry(ctx context.Context, hash string, signedTxXDR string, retryOptions ...retry.Option) error {
	attemptsCount := 0
	outerErr := retry.Do(
		func() error {
			attemptsCount++
			result, err := s.RPCService.SendTransaction(signedTxXDR)
			if err != nil {
				return fmt.Errorf("sending transaction with hash %q: %w", hash, err)
			}

			switch result.Status {
			case entities.PendingStatus:
				return nil
			case entities.ErrorStatus:
				err = fmt.Errorf("transaction with hash %q failed with errorResultXdr %s", hash, result.ErrorResultXDR)
				return retry.Unrecoverable(err)
			case entities.TryAgainLaterStatus:
				return fmt.Errorf("received TryAgainLaterStatus, retrying...")
			default:
				return fmt.Errorf("unexpected transaction status: %s", result.Status)
			}
		},
		append(
			retryOptions,
			retry.Context(ctx),
			retry.LastErrorOnly(true),
		)...,
	)

	if outerErr != nil {
		return fmt.Errorf("transaction did not complete after %d attempts: %w", attemptsCount, outerErr)
	}

	return nil
}

// waitForTransactionConfirmation waits for a transaction with the provided hash to be confirmed.
func (s *channelAccountService) waitForTransactionConfirmation(ctx context.Context, hash string, retryOptions ...retry.Option) error {
	attemptsCount := 0
	outerErr := retry.Do(
		func() error {
			attemptsCount++
			txResult, err := s.RPCService.GetTransaction(hash)
			if err != nil {
				return fmt.Errorf("getting transaction with hash %q: %w", hash, err)
			}

			switch txResult.Status {
			case entities.NotFoundStatus:
				return fmt.Errorf("transaction not found")
			case entities.SuccessStatus:
				return nil
			case entities.FailedStatus:
				err = fmt.Errorf("transaction with hash %q failed with status %s and errorResultXdr %s", hash, txResult.Status, txResult.ErrorResultXDR)
				return retry.Unrecoverable(err)
			default:
				return fmt.Errorf("unexpected transaction status: %s", txResult.Status)
			}
		},
		append(
			retryOptions,
			retry.Context(ctx),
			retry.LastErrorOnly(true),
		)...,
	)

	if outerErr != nil {
		return fmt.Errorf("failed to get transaction status after %d attempts: %w", attemptsCount, outerErr)
	}

	return nil
}

// // waitForTransactionConfirmation waits for a transaction with the provided hash to be confirmed.
// func (s *channelAccountService) waitForTransactionConfirmation(_ context.Context, hash string, maxRetries int) error {
// 	for range maxRetries {
// 		txResult, err := s.RPCService.GetTransaction(hash)
// 		if err != nil {
// 			return fmt.Errorf("getting transaction with hash %q: %w", hash, err)
// 		}

// 		//exhaustive:ignore
// 		switch txResult.Status {
// 		case entities.NotFoundStatus:
// 			time.Sleep(sleepDelayForChannelAccountCreation)
// 			continue
// 		case entities.SuccessStatus:
// 			return nil
// 		case entities.FailedStatus:
// 			return fmt.Errorf("transaction with hash %q failed with status %s and errorResultXdr %s", hash, txResult.Status, txResult.ErrorResultXDR)
// 		}
// 	}

// 	return fmt.Errorf("failed to get transaction status after %d attempts", maxRetries)
// }

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
		return nil, fmt.Errorf("validating channel account service options: %w", err)
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
