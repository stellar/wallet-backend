package services

import (
	"context"
	"fmt"
	"io"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

const (
	archiveURL = "https://history.stellar.org/prd/core-live/core_live_001/"
)

type trustlinesService struct {
	archive    historyarchive.ArchiveInterface
}

func NewTrustlinesService(networkPassphrase string) (*trustlinesService, error) {
	archive, err := historyarchive.Connect(
		archiveURL,
		historyarchive.ArchiveOptions{
			NetworkPassphrase: networkPassphrase,
		},
	)
	if err != nil {
		return nil, err
	}
	return &trustlinesService{archive: archive}, nil
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
		return err
	}
	defer reader.Close()

	// trustlines is a map of account address to a list of asset codes
	trustlines := make(map[string][]string, 0)
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
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
			trustlines[accountAddress] = append(trustlines[accountAddress], fmt.Sprintf("%s:%s", assetCode, assetIssuer))
		default:
			continue
		}
	}
	return nil
}

func (s *trustlinesService) getLatestCheckpointLedger() (uint32, error) {
	// Get latest ledger from archive
	latestLedger, err := s.archive.GetLatestLedgerSequence()
	if err != nil {
		return 0, err
	}

	// Get checkpoint manager
	manager := s.archive.GetCheckpointManager()

	// Return the latest checkpoint (on or before latest ledger)
	if manager.IsCheckpoint(latestLedger) {
		return latestLedger, nil
	}
	return manager.PrevCheckpoint(latestLedger), nil
}
