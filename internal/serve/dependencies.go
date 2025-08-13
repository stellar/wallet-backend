package serve

import (
	"context"
	"net/http"

	"github.com/jmoiron/sqlx"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	txservices "github.com/stellar/wallet-backend/internal/transactions/services"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

// DatabaseProvider provides database connections and models
type DatabaseProvider interface {
	GetDB(ctx context.Context) (*sqlx.DB, error)
	GetModels() (*data.Models, error)
}

// HTTPClientProvider provides HTTP clients
type HTTPClientProvider interface {
	GetClient() *http.Client
}

// AuthProvider provides authentication components
type AuthProvider interface {
	GetRequestVerifier() auth.HTTPRequestVerifier
}

// ServiceDependencies holds the basic dependencies needed for service creation
type ServiceDependencies struct {
	DatabaseProvider                   DatabaseProvider
	HTTPClientProvider                 HTTPClientProvider
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	NetworkPassphrase                  string
	BaseFee                            int64
	MaxSponsoredBaseReserves           int
	EncryptionPassphrase               string
	RPCURL                             string
	ServerBaseURL                      string
	SupportedAssets                    []entities.Asset
	AppTracker                         apptracker.AppTracker
}

// ServiceContainer manages all business services
type ServiceContainer interface {
	GetAccountService() services.AccountService
	GetAccountSponsorshipService() services.AccountSponsorshipService
	GetChannelAccountService() services.ChannelAccountService
	GetPaymentService() services.PaymentService
	GetRPCService() services.RPCService
	GetTransactionService() txservices.TransactionService
	GetMetricsService() metrics.MetricsService
	GetModels() *data.Models
	GetSupportedAssets() []entities.Asset
	GetNetworkPassphrase() string
	GetServerHostname() string
	GetAppTracker() apptracker.AppTracker
}

// HandlerDependencies represents all dependencies needed for HTTP handlers
type HandlerDependencies struct {
	ServiceContainer ServiceContainer
	AuthProvider     AuthProvider
}
