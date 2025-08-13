package serve

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

// databaseProvider implements DatabaseProvider
type databaseProvider struct {
	connectionPool db.ConnectionPool
	models         *data.Models
}

func NewDatabaseProvider(databaseURL string, metricsService metrics.MetricsService) (*databaseProvider, error) {
	connectionPool, err := db.OpenDBConnectionPool(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("opening database connection pool: %w", err)
	}

	models, err := data.NewModels(connectionPool, metricsService)
	if err != nil {
		return nil, fmt.Errorf("creating data models: %w", err)
	}

	return &databaseProvider{
		connectionPool: connectionPool,
		models:         models,
	}, nil
}

func (p *databaseProvider) GetDB(ctx context.Context) (*sqlx.DB, error) {
	db, err := p.connectionPool.SqlxDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting sqlx DB: %w", err)
	}
	return db, nil
}

func (p *databaseProvider) GetModels() (*data.Models, error) {
	return p.models, nil
}

// httpClientProvider implements HTTPClientProvider
type httpClientProvider struct {
	client *http.Client
}

func NewHTTPClientProvider() *httpClientProvider {
	return &httpClientProvider{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (p *httpClientProvider) GetClient() *http.Client {
	return p.client
}

// authProvider implements AuthProvider
type authProvider struct {
	requestVerifier auth.HTTPRequestVerifier
}

func NewAuthProvider(clientAuthPublicKeys []string) (*authProvider, error) {
	jwtTokenParser, err := auth.NewMultiJWTTokenParser(time.Second*5, clientAuthPublicKeys...)
	if err != nil {
		return nil, fmt.Errorf("creating JWT token parser: %w", err)
	}
	requestVerifier := auth.NewHTTPRequestVerifier(jwtTokenParser, auth.DefaultMaxBodySize)

	return &authProvider{
		requestVerifier: requestVerifier,
	}, nil
}

func (p *authProvider) GetRequestVerifier() auth.HTTPRequestVerifier {
	return p.requestVerifier
}

