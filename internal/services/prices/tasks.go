package prices

import (
	"context"
	"net/http"
	"time"

	"github.com/stellar/go-stellar-sdk/network"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// LiveTasksConfig wires the price background tasks for the live ingester.
type LiveTasksConfig struct {
	Venues            processors.TradeVenues
	Anchor            *Anchor
	Models            *data.Models
	Metadata          ContractFieldFetcher
	NetworkPassphrase string
	OracleInterval    time.Duration
	CompareInterval   time.Duration
	StellarExpertURL  string
	// Metrics is optional.
	Metrics *metrics.PricesMetrics
}

// LiveTasks returns the background tasks to start after the live lock is held: the oracle anchor
// poller, and the comparison sampler when its interval is positive.
func LiveTasks(cfg LiveTasksConfig) ([]func(context.Context), error) {
	var tasks []func(context.Context)
	poller, err := NewOraclePoller(OracleConfig{
		OracleContractID: OracleContractFor(cfg.NetworkPassphrase),
		Metadata:         cfg.Metadata,
		Interval:         cfg.OracleInterval,
		Anchor:           cfg.Anchor,
		OraclePrices:     cfg.Models.OraclePrices,
		XLMSAC:           cfg.Venues.XLMSAC,
		USDCSAC:          cfg.Venues.USDCSAC,
		Metrics:          cfg.Metrics,
	})
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, poller.Run)
	if cfg.CompareInterval > 0 {
		sampler := NewComparisonSampler(ComparisonConfig{
			DB:       cfg.Models.DB,
			Store:    cfg.Models.PriceComparisons,
			Client:   NewStellarExpertClient(cfg.StellarExpertURL, &http.Client{Timeout: 10 * time.Second}),
			XLMSAC:   cfg.Venues.XLMSAC,
			Interval: cfg.CompareInterval,
			Metrics:  cfg.Metrics,
		})
		tasks = append(tasks, sampler.Run)
	}
	return tasks, nil
}

// reflectorCEXOracles maps a network passphrase to the Reflector external CEX/DEX feed, whose
// base is USD and which quotes both anchor tokens.
var reflectorCEXOracles = map[string]string{
	network.PublicNetworkPassphrase: "CAFJZQWSED6YAWZU3GWRTOCNPPCGBN32L7QV43XX5LZLFTK6JLN34DLN",
}

// OracleContractFor returns the anchor oracle for a network, or "" when none is configured.
func OracleContractFor(networkPassphrase string) string {
	return reflectorCEXOracles[networkPassphrase]
}
