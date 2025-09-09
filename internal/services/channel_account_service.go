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
		log.Ctx(ctx).Infof("✅ There are exactly %d channel accounts currently. Skipping...", number)
		return nil
	} else if currentChannelAccountNumber > number {
		// Delete excess accounts
		numAccountsToDelete := currentChannelAccountNumber - number
		log.Ctx(ctx).Infof("⏳ Deleting %d excess channel accounts...", numAccountsToDelete)
		return s.deleteChannelAccountsInBatches(ctx, int(numAccountsToDelete))
	} else {
		// Create additional accounts
		numOfChannelAccountsToCreate := number - currentChannelAccountNumber
		log.Ctx(ctx).Infof("⏳ Creating %d additional channel accounts...", numOfChannelAccountsToCreate)
		return s.createChannelAccountsInBatches(ctx, int(numOfChannelAccountsToCreate))
	}
}

// createChannelAccountsInBatches creates channel accounts in batches of MaximumCreateAccountOperationsPerStellarTx (19).
// It is used to create more than 19 channel accounts by submitting multiple transactions.
func (s *channelAccountService) createChannelAccountsInBatches(ctx context.Context, amount int) error {
	for amount > 0 {
		batchSize := min(amount, MaximumCreateAccountOperationsPerStellarTx)
		log.Ctx(ctx).Debugf("batch size: %d", batchSize)
		err := s.createChannelAccounts(ctx, batchSize)
		if err != nil {
			return fmt.Errorf("creating channel accounts in batch: %w", err)
		}
		amount -= batchSize
	}

	return nil
}

// createChannelAccounts creates channel accounts in a single transaction. The maximum amount of channel accounts that
// can be created in a single transaction is MaximumCreateAccountOperationsPerStellarTx (19), due to payload size limit.
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

	chAccSigner := func(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.Transaction, error) {
		return tx.Sign(s.ChannelAccountSignatureClient.NetworkPassphrase(), chAccKPs...)
	}

	err = s.submitChannelAccountsTxOnChain(ctx, distributionAccountPublicKey, ops, chAccSigner)
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

// deleteChannelAccountsInBatches deletes channel accounts in batches of MaximumCreateAccountOperationsPerStellarTx (19).
// It is used to delete more than 19 channel accounts by submitting multiple transactions.
func (s *channelAccountService) deleteChannelAccountsInBatches(ctx context.Context, amount int) error {
	for amount > 0 {
		batchSize := min(amount, MaximumCreateAccountOperationsPerStellarTx)
		log.Ctx(ctx).Debugf("batch size: %d", batchSize)
		err := s.deleteChannelAccounts(ctx, batchSize)
		if err != nil {
			return fmt.Errorf("deleting channel accounts in batch: %w", err)
		}
		amount -= batchSize
	}

	return nil
}

// deleteChannelAccounts deletes channel accounts in a single transaction. The maximum amount of channel accounts that
// can be deleted in a single transaction is MaximumCreateAccountOperationsPerStellarTx (19), due to payload size limit.
func (s *channelAccountService) deleteChannelAccounts(ctx context.Context, numAccountsToDelete int) error {
	dbTxErr := db.RunInTransaction(ctx, s.DB, nil, func(dbTx db.Transaction) error {
		// Get a channel account to delete
		chAccounts, err := s.ChannelAccountStore.GetAll(ctx, dbTx, numAccountsToDelete)
		if err != nil {
			return fmt.Errorf("retrieving channel account to delete: %w", err)
		}

		distAccPublicKey, err := s.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
		if err != nil {
			return fmt.Errorf("getting distribution account public key: %w", err)
		}

		var ops []txnbuild.Operation
		for _, chAcc := range chAccounts {
			_, err = s.RPCService.GetAccountLedgerSequence(chAcc.PublicKey)
			if errors.Is(err, ErrAccountNotFound) {
				log.Ctx(ctx).Infof("⏭️ Account %s does not exist on the network, proceeding to delete it from the database", chAcc.PublicKey)
				continue
			}

			ops = append(ops, &txnbuild.AccountMerge{
				Destination:   distAccPublicKey,
				SourceAccount: chAcc.PublicKey,
			})
		}

		if len(ops) > 0 {
			publicKeys := make([]string, len(chAccounts))
			for i, chAcc := range chAccounts {
				publicKeys[i] = chAcc.PublicKey
			}

			chAccSigner := func(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.Transaction, error) {
				tx, err = s.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, publicKeys...)
				if err != nil {
					return nil, fmt.Errorf("signing delete account transaction: %w", err)
				}
				return tx, nil
			}

			log.Ctx(ctx).Infof("⛓️ Will merge accounts on chain: %v", publicKeys)
			err = s.submitChannelAccountsTxOnChain(ctx, distAccPublicKey, ops, chAccSigner)
			if err != nil {
				return fmt.Errorf("submitting delete account transaction: %w", err)
			}

			if _, err = s.ChannelAccountStore.Delete(ctx, dbTx, publicKeys...); err != nil {
				return fmt.Errorf("deleting channel accounts from database: %w", err)
			}
			log.Ctx(ctx).Infof("✅ Successfully deleted channel accounts from the database %v", publicKeys)
		}

		return nil
	})
	if dbTxErr != nil {
		return fmt.Errorf("deleting channel accounts in dbTx: %w", dbTxErr)
	}

	return nil
}

type ChannelAccSigner func(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.Transaction, error)

func (s *channelAccountService) submitChannelAccountsTxOnChain(
	ctx context.Context,
	distributionAccountPublicKey string,
	ops []txnbuild.Operation,
	chAccSigner ChannelAccSigner,
) error {
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

		// Sign the transaction for the distribution account
		tx, err = s.DistributionAccountSignatureClient.SignStellarTransaction(ctx, tx, distributionAccountPublicKey)
		if err != nil {
			return fmt.Errorf("signing transaction for distribution account: %w", err)
		}
		// Sign the transaction for the channel accounts
		tx, err = chAccSigner(ctx, tx)
		if err != nil {
			return fmt.Errorf("signing transaction with channel account(s) keypairs: %w", err)
		}

		txHash, err := tx.HashHex(s.DistributionAccountSignatureClient.NetworkPassphrase())
		if err != nil {
			return fmt.Errorf("getting transaction hash: %w", err)
		}
		txXDR, err := tx.Base64()
		if err != nil {
			return fmt.Errorf("getting transaction envelope: %w", err)
		}

		log.Ctx(ctx).Infof("🚧 Submitting channel account transaction to RPC service")
		err = s.submitTransaction(ctx, txHash, txXDR)
		if err != nil {
			return fmt.Errorf("submitting channel account transaction to RPC service: %w", err)
		}
		log.Ctx(ctx).Infof("🚧 Successfully submitted channel account transaction to RPC service, waiting for confirmation...")
		err = s.waitForTransactionConfirmation(ctx, txHash)
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
		log.Errorf("error unmarshalling transaction result: %v", err)
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
