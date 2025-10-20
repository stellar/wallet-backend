package services

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/store"
)

const (
	archiveURL = "https://history.stellar.org/prd/core-live/core_live_001/"
)

// TrustlinesService defines the interface for trustlines operations.
type TrustlinesService interface {
	PopulateTrustlines(ctx context.Context) error
	AddTrustlines(ctx context.Context, accountAddress string, assets []string) error
	GetTrustlines(ctx context.Context, accountAddress string) ([]string, error)
	HasTrustline(ctx context.Context, accountAddress string, asset string) (bool, error)
	RemoveTrustlines(ctx context.Context, accountAddress string, assets []string) error
}

var _ TrustlinesService = (*trustlinesService)(nil)

type trustlinesService struct {
	archive          historyarchive.ArchiveInterface
	redisStore       *store.RedisStore
	trustlinesPrefix string
}

func NewTrustlinesService(networkPassphrase string, redisStore *store.RedisStore) (TrustlinesService, error) {
	archive, err := historyarchive.Connect(
		archiveURL,
		historyarchive.ArchiveOptions{
			NetworkPassphrase: networkPassphrase,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to history archive: %w", err)
	}
	return &trustlinesService{
		archive:          archive,
		redisStore:       redisStore,
		trustlinesPrefix: "trustlines:",
	}, nil
}

// AddTrustlines adds trustlines for an account to Redis.
func (s *trustlinesService) AddTrustlines(ctx context.Context, accountAddress string, assets []string) error {
	if len(assets) == 0 {
		return nil
	}
	key := s.trustlinesPrefix + accountAddress
	if err := s.redisStore.SAdd(ctx, key, assets...); err != nil {
		return fmt.Errorf("adding trustlines for account %s: %w", accountAddress, err)
	}
	return nil
}

// GetTrustlines retrieves all trustlines for an account from Redis.
func (s *trustlinesService) GetTrustlines(ctx context.Context, accountAddress string) ([]string, error) {
	key := s.trustlinesPrefix + accountAddress
	trustlines, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting trustlines for account %s: %w", accountAddress, err)
	}
	return trustlines, nil
}

// HasTrustline checks if an account has a specific trustline.
func (s *trustlinesService) HasTrustline(ctx context.Context, accountAddress string, asset string) (bool, error) {
	key := s.trustlinesPrefix + accountAddress
	hasTrustline, err := s.redisStore.SIsMember(ctx, key, asset)
	if err != nil {
		return false, fmt.Errorf("checking trustline for account %s: %w", accountAddress, err)
	}
	return hasTrustline, nil
}

// RemoveTrustlines removes a list of trustlines from an account.
func (s *trustlinesService) RemoveTrustlines(ctx context.Context, accountAddress string, assets []string) error {
	key := s.trustlinesPrefix + accountAddress
	if err := s.redisStore.SRem(ctx, key, assets...); err != nil {
		return fmt.Errorf("removing trustline for account %s: %w", accountAddress, err)
	}
	return nil
}

func (s *trustlinesService) PopulateTrustlines(ctx context.Context) error {
	latestCheckpointLedger, err := s.getLatestCheckpointLedger()
	if err != nil {
		return err
	}

	reader, err := ingest.NewCheckpointChangeReader(
		context.Background(),
		s.archive,
		latestCheckpointLedger,
	)
	if err != nil {
		return fmt.Errorf("creating checkpoint change reader: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			// Log error but don't override the function's return error
			fmt.Printf("error closing reader: %v\n", closeErr)
		}
	}()

	// trustlines is a map of account address to a list of asset codes
	trustlines := make(map[string][]string, 0)
	for {
		change, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading checkpoint changes: %w", err)
		}

		switch change.Type {
		case xdr.LedgerEntryTypeTrustline:
			trustlineEntry := change.Post.Data.MustTrustLine()
			accountAddress := trustlineEntry.AccountId.Address()
			var assetType, assetCode, assetIssuer string
			err = trustlineEntry.Asset.Extract(&assetType, &assetCode, &assetIssuer)
			if err != nil {
				continue
			}
			assetStr := fmt.Sprintf("%s:%s", assetCode, assetIssuer)
			trustlines[accountAddress] = append(trustlines[accountAddress], assetStr)
		default:
			continue
		}
	}

	// Store trustlines in Redis
	for accountAddress, assets := range trustlines {
		if err := s.AddTrustlines(ctx, accountAddress, assets); err != nil {
			return fmt.Errorf("storing trustlines for account %s: %w", accountAddress, err)
		}
	}

	return nil
}

func (s *trustlinesService) getLatestCheckpointLedger() (uint32, error) {
	// Get latest ledger from archive
	latestLedger, err := s.archive.GetLatestLedgerSequence()
	if err != nil {
		return 0, fmt.Errorf("getting latest ledger sequence: %w", err)
	}

	// Get checkpoint manager
	manager := s.archive.GetCheckpointManager()

	// Return the latest checkpoint (on or before latest ledger)
	if manager.IsCheckpoint(latestLedger) {
		return latestLedger, nil
	}
	return manager.PrevCheckpoint(latestLedger), nil
}
