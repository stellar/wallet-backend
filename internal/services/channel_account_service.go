package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
)

const (
	// MaximumCreateAccountOperationsPerStellarTx is the max number of sponsored accounts we can create in one
	// transaction due to the signature limit.
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
	ChannelAccountSignatureClient      signing.SignatureClient
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

	log.Ctx(ctx).Infof("🔍 Channel accounts amounts: {desired:%d, existing:%d}", number, currentChannelAccountNumber)

	if currentChannelAccountNumber == number {
		log.Ctx(ctx).Infof("✅ There are exactly %d channel accounts currently. Exiting...", number)
		return nil
	} else if currentChannelAccountNumber > number {
		// Delete excess accounts
		numAccountsToDelete := currentChannelAccountNumber - number
		log.Ctx(ctx).Infof("⏳ Deleting %d excess channel accounts...", numAccountsToDelete)
		return s.deleteChannelAccounts(ctx, int(numAccountsToDelete))
	} else {
		// Create additional accounts
		numOfChannelAccountsToCreate := number - currentChannelAccountNumber
		log.Ctx(ctx).Infof("⏳ Creating %d additional channel accounts...", numOfChannelAccountsToCreate)
		return s.createChannelAccounts(ctx, int(numOfChannelAccountsToCreate))
	}
}

func (s *channelAccountService) createChannelAccounts(ctx context.Context, amount int) error {
	if amount > MaximumCreateAccountOperationsPerStellarTx {
		return fmt.Errorf("number of channel accounts to create is greater than the maximum allowed per transaction (%d)", MaximumCreateAccountOperationsPerStellarTx)
	}

	distributionAccountPublicKey, err := s.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return fmt.Errorf("getting distribution account public key: %w", err)
	}

	ops := make([]txnbuild.Operation, 0, amount)
	channelAccountsToInsert := []*store.ChannelAccount{}
	chAccKPs := make([]*keypair.Full, 0, amount)
	for range amount {
		kp, innerErr := keypair.Random()
		if innerErr != nil {
			return fmt.Errorf("generating random keypair for channel account: %w", innerErr)
		}
		chAccKPs = append(chAccKPs, kp)
		log.Ctx(ctx).Infof("⏳ Creating Stellar channel account with address: %s", kp.Address())

		encryptedPrivateKey, innerErr := s.PrivateKeyEncrypter.Encrypt(ctx, kp.Seed(), s.EncryptionPassphrase)
		if innerErr != nil {
			return fmt.Errorf("encrypting channel account private key: %w", innerErr)
		}

		ops = append(ops, // add sponsor operations for this account
			&txnbuild.BeginSponsoringFutureReserves{
				SponsoredID: kp.Address(),
			},
			&txnbuild.CreateAccount{
				Destination: kp.Address(),
				Amount:      "0",
			},
			&txnbuild.EndSponsoringFutureReserves{
				SourceAccount: kp.Address(),
			},
		)
		channelAccountsToInsert = append(channelAccountsToInsert, &store.ChannelAccount{
			PublicKey:           kp.Address(),
			EncryptedPrivateKey: encryptedPrivateKey,
		})
	}

	err = s.submitCreateChannelAccountsOnChainTransaction(ctx, distributionAccountPublicKey, ops, chAccKPs...)
	if err != nil {
		return fmt.Errorf("submitting create channel account(s) on chain transaction: %w", err)
	}
	log.Ctx(ctx).Infof("🎉 Successfully created %d channel account(s) on chain", amount)

	if err = s.ChannelAccountStore.BatchInsert(ctx, s.DB, channelAccountsToInsert); err != nil {
		return fmt.Errorf("inserting channel account(s): %w", err)
	}
	log.Ctx(ctx).Infof("✅ Successfully stored %d channel account(s) into the store", amount)

	return nil
}

func (s *channelAccountService) deleteChannelAccounts(ctx context.Context, numAccountsToDelete int) error {
	// Get a channel account to delete
	chAccounts, err := s.ChannelAccountStore.GetAll(ctx, s.DB, numAccountsToDelete)
	if err != nil {
		return fmt.Errorf("retrieving channel account to delete: %w", err)
	}

	for _, chAcc := range chAccounts {
		err = s.deleteChannelAccount(ctx, chAcc)
		if err != nil {
			return fmt.Errorf("deleting channel account %s: %w", chAcc.PublicKey, err)
		}
	}

	return nil
}

func (s *channelAccountService) deleteChannelAccount(ctx context.Context, chAcc *store.ChannelAccount) (err error) {
	// Remove from database after successful on-chain deletion
	defer func() {
		if err == nil {
			err = s.ChannelAccountStore.Delete(ctx, s.DB, chAcc.PublicKey)
			if err != nil {
				err = fmt.Errorf("deleting %s from database: %w", chAcc.PublicKey, err)
			}
		}
	}()

	// Check if account exists on the network
	_, err = s.RPCService.GetAccountLedgerSequence(chAcc.PublicKey)
	if errors.Is(err, ErrAccountNotFound) {
		log.Ctx(ctx).Infof("⏭️ Account %s does not exist on the network, proceeding to delete it from the database", chAcc.PublicKey)
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting account ledger sequence for %s: %w", chAcc.PublicKey, err)
	}
	log.Ctx(ctx).Infof("⏳ Deleting Stellar account with address: %s", chAcc.PublicKey)

	// Create account merge operation to delete the account
	distAccPublicKey, err := s.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return fmt.Errorf("getting distribution account public key: %w", err)
	}
	accountSeq, err := s.RPCService.GetAccountLedgerSequence(distAccPublicKey)
	if err != nil {
		return fmt.Errorf("getting ledger sequence for distribution account %s: %w", distAccPublicKey, err)
	}

	// Build the tx
	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: distAccPublicKey,
				Sequence:  accountSeq,
			},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.AccountMerge{
					Destination:   distAccPublicKey,
					SourceAccount: chAcc.PublicKey,
				},
			},
			BaseFee:       s.BaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		},
	)
	if err != nil {
		return fmt.Errorf("building delete account transaction: %w", err)
	}

	// Sign the tx
	tx, err = s.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, chAcc.PublicKey)
	if err != nil {
		return fmt.Errorf("signing delete account transaction: %w", err)
	}
	tx, err = s.DistributionAccountSignatureClient.SignStellarTransaction(ctx, tx, distAccPublicKey)
	if err != nil {
		return fmt.Errorf("signing delete account transaction: %w", err)
	}

	// Get the tx hash and XDR
	txHash, err := tx.HashHex(s.ChannelAccountSignatureClient.NetworkPassphrase())
	if err != nil {
		return fmt.Errorf("getting transaction hash: %w", err)
	}
	txXDR, err := tx.Base64()
	if err != nil {
		return fmt.Errorf("getting transaction envelope: %w", err)
	}

	// Submit the tx and wait for confirmation
	log.Ctx(ctx).Infof("🚧 Submitting delete channel account transaction to rpc service")
	err = s.submitTransaction(ctx, txHash, txXDR)
	if err != nil {
		return fmt.Errorf("submitting delete channel account transaction: %w", err)
	}
	log.Ctx(ctx).Infof("⏳ Successfully submitted delete channel account transaction, waiting for confirmation...")
	err = s.waitForTransactionConfirmation(ctx, txHash)
	if err != nil {
		return fmt.Errorf("waiting for delete transaction confirmation: %w", err)
	}
	log.Ctx(ctx).Infof("✅ Successfully deleted channel account %s", chAcc.PublicKey)

	return nil
}

func (s *channelAccountService) submitCreateChannelAccountsOnChainTransaction(ctx context.Context, distributionAccountPublicKey string, ops []txnbuild.Operation, chAccKPs ...*keypair.Full) error {
	log.Ctx(ctx).Infof("⏳ Waiting for RPC service to become healthy")
	rpcHeartbeatChannel := s.RPCService.GetHeartbeatChannel()
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for rpc service to become healthy: %w", ctx.Err())
	// The channel account creation goroutine will wait in the background for the rpc service to become healthy on startup.
	// This lets the API server startup so that users can start interacting with the API which does not depend on RPC, instead of waiting till it becomes healthy.
	case <-rpcHeartbeatChannel:
		log.Ctx(ctx).Infof("👍 RPC service is healthy")
		accountSeq, err := s.RPCService.GetAccountLedgerSequence(distributionAccountPublicKey)
		if err != nil {
			return fmt.Errorf("getting ledger sequence for distribution account public key=%s: %w", distributionAccountPublicKey, err)
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
			return fmt.Errorf("signing transaction for distribution account: %w", err)
		}

		if len(chAccKPs) > 0 {
			signedTx, err = signedTx.Sign(s.DistributionAccountSignatureClient.NetworkPassphrase(), chAccKPs...)
			if err != nil {
				return fmt.Errorf("signing transaction with channel account(s) keypairs: %w", err)
			}
		}

		hash, err := signedTx.HashHex(s.DistributionAccountSignatureClient.NetworkPassphrase())
		if err != nil {
			return fmt.Errorf("getting transaction hash: %w", err)
		}

		signedTxXDR, err := signedTx.Base64()
		if err != nil {
			return fmt.Errorf("getting transaction envelope: %w", err)
		}

		log.Ctx(ctx).Infof("🚧 Submitting channel account transaction to rpc service")
		err = s.submitTransaction(ctx, hash, signedTxXDR)
		if err != nil {
			return fmt.Errorf("submitting channel account transaction to rpc service: %w", err)
		}

		log.Ctx(ctx).Infof("🚧 Successfully submitted channel account transaction to rpc service, waiting for confirmation...")
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
			return fmt.Errorf("transaction with hash %q failed with errorResultXdr %s", hash, parseResultXDR(result.ErrorResultXDR))
		case entities.TryAgainLaterStatus:
			time.Sleep(sleepDelayForChannelAccountCreation)
			continue
		}
	}

	return fmt.Errorf("transaction did not complete after %d attempts", maxRetriesForChannelAccountCreation)
}

func parseResultXDR(resultXDR string) string {
	var xdrResult xdr.TransactionResult
	err := xdr.SafeUnmarshalBase64(resultXDR, &xdrResult)
	if err != nil {
		log.Errorf("unmarshalling transaction result=%v", err)
		return resultXDR
	}
	return fmt.Sprintf("%+v", xdrResult)
}

func (s *channelAccountService) waitForTransactionConfirmation(_ context.Context, hash string) error {
	for range maxRetriesForChannelAccountCreation {
		txResult, err := s.RPCService.GetTransaction(hash)
		if err != nil {
			return fmt.Errorf("getting transaction with hash %q: %w", hash, err)
		}

		//exhaustive:ignore
		switch txResult.Status {
		case entities.NotFoundStatus:
			time.Sleep(sleepDelayForChannelAccountCreation)
			continue
		case entities.SuccessStatus:
			return nil
		case entities.FailedStatus:
			return fmt.Errorf("transaction with hash %q failed with status %s and errorResultXdr %s", hash, txResult.Status, txResult.ErrorResultXDR)
		}
	}

	return fmt.Errorf("failed to get transaction status after %d attempts", maxRetriesForChannelAccountCreation)
}

type ChannelAccountServiceOptions struct {
	DB                                 db.ConnectionPool
	RPCService                         RPCService
	BaseFee                            int64
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
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
	err := opts.Validate()
	if err != nil {
		return nil, fmt.Errorf("validating channel account service options: %w", err)
	}

	go opts.RPCService.TrackRPCServiceHealth(ctx, nil)

	return &channelAccountService{
		DB:                                 opts.DB,
		RPCService:                         opts.RPCService,
		BaseFee:                            opts.BaseFee,
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      opts.ChannelAccountSignatureClient,
		ChannelAccountStore:                opts.ChannelAccountStore,
		PrivateKeyEncrypter:                opts.PrivateKeyEncrypter,
		EncryptionPassphrase:               opts.EncryptionPassphrase,
	}, nil
}
