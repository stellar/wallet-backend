package serve

import (
	"context"
	"fmt"
	"net/url"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
	txservices "github.com/stellar/wallet-backend/internal/transactions/services"
)


// serviceContainer implements ServiceContainer
type serviceContainer struct {
	accountService            services.AccountService
	accountSponsorshipService services.AccountSponsorshipService
	channelAccountService     services.ChannelAccountService
	paymentService            services.PaymentService
	rpcService                services.RPCService
	transactionService        txservices.TransactionService
	metricsService            metrics.MetricsService
	models                    *data.Models
	supportedAssets           []entities.Asset
	networkPassphrase         string
	serverHostname            string
	appTracker                apptracker.AppTracker
}

func (c *serviceContainer) GetAccountService() services.AccountService {
	return c.accountService
}

func (c *serviceContainer) GetAccountSponsorshipService() services.AccountSponsorshipService {
	return c.accountSponsorshipService
}

func (c *serviceContainer) GetChannelAccountService() services.ChannelAccountService {
	return c.channelAccountService
}

func (c *serviceContainer) GetPaymentService() services.PaymentService {
	return c.paymentService
}

func (c *serviceContainer) GetRPCService() services.RPCService {
	return c.rpcService
}

func (c *serviceContainer) GetTransactionService() txservices.TransactionService {
	return c.transactionService
}

func (c *serviceContainer) GetMetricsService() metrics.MetricsService {
	return c.metricsService
}

func (c *serviceContainer) GetModels() *data.Models {
	return c.models
}

func (c *serviceContainer) GetSupportedAssets() []entities.Asset {
	return c.supportedAssets
}

func (c *serviceContainer) GetNetworkPassphrase() string {
	return c.networkPassphrase
}

func (c *serviceContainer) GetServerHostname() string {
	return c.serverHostname
}

func (c *serviceContainer) GetAppTracker() apptracker.AppTracker {
	return c.appTracker
}

// NewServiceContainer creates a new service container with all required services
func NewServiceContainer(ctx context.Context, deps ServiceDependencies) (*serviceContainer, error) {
	models, err := deps.DatabaseProvider.GetModels()
	if err != nil {
		return nil, fmt.Errorf("getting models: %w", err)
	}

	db, err := deps.DatabaseProvider.GetDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting database: %w", err)
	}

	metricsService := metrics.NewMetricsService(db)

	rpcService, err := createRPCService(deps, metricsService)
	if err != nil {
		return nil, fmt.Errorf("creating RPC service: %w", err)
	}

	accountService, err := createAccountService(models, metricsService)
	if err != nil {
		return nil, fmt.Errorf("creating account service: %w", err)
	}

	accountSponsorshipService, err := createAccountSponsorshipService(deps, models, rpcService)
	if err != nil {
		return nil, fmt.Errorf("creating account sponsorship service: %w", err)
	}

	channelAccountService, err := createChannelAccountService(ctx, deps, rpcService)
	if err != nil {
		return nil, fmt.Errorf("creating channel account service: %w", err)
	}

	paymentService, err := createPaymentService(models, deps.ServerBaseURL)
	if err != nil {
		return nil, fmt.Errorf("creating payment service: %w", err)
	}

	transactionService, err := createTransactionService(deps, rpcService)
	if err != nil {
		return nil, fmt.Errorf("creating transaction service: %w", err)
	}

	serverHostname, err := extractHostname(deps.ServerBaseURL)
	if err != nil {
		return nil, fmt.Errorf("extracting hostname: %w", err)
	}

	return &serviceContainer{
		accountService:            accountService,
		accountSponsorshipService: accountSponsorshipService,
		channelAccountService:     channelAccountService,
		paymentService:            paymentService,
		rpcService:                rpcService,
		transactionService:        transactionService,
		metricsService:            metricsService,
		models:                    models,
		supportedAssets:           deps.SupportedAssets,
		networkPassphrase:         deps.NetworkPassphrase,
		serverHostname:            serverHostname,
		appTracker:                deps.AppTracker,
	}, nil
}

func createRPCService(deps ServiceDependencies, metricsService metrics.MetricsService) (services.RPCService, error) {
	httpClient := deps.HTTPClientProvider.GetClient()
	rpcService, err := services.NewRPCService(deps.RPCURL, deps.NetworkPassphrase, httpClient, metricsService)
	if err != nil {
		return nil, fmt.Errorf("creating RPC service: %w", err)
	}
	return rpcService, nil
}

func createAccountService(models *data.Models, metricsService metrics.MetricsService) (services.AccountService, error) {
	accountService, err := services.NewAccountService(models, metricsService)
	if err != nil {
		return nil, fmt.Errorf("creating account service: %w", err)
	}
	return accountService, nil
}

func createAccountSponsorshipService(deps ServiceDependencies, models *data.Models, rpcService services.RPCService) (services.AccountSponsorshipService, error) {
	accountSponsorshipService, err := services.NewAccountSponsorshipService(services.AccountSponsorshipServiceOptions{
		DistributionAccountSignatureClient: deps.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      deps.ChannelAccountSignatureClient,
		RPCService:                         rpcService,
		MaxSponsoredBaseReserves:           deps.MaxSponsoredBaseReserves,
		BaseFee:                            deps.BaseFee,
		Models:                             models,
		BlockedOperationsTypes:             blockedOperationTypes,
	})
	if err != nil {
		return nil, fmt.Errorf("creating account sponsorship service: %w", err)
	}
	return accountSponsorshipService, nil
}

func createChannelAccountService(ctx context.Context, deps ServiceDependencies, rpcService services.RPCService) (services.ChannelAccountService, error) {
	dataModels, err := deps.DatabaseProvider.GetModels()
	if err != nil {
		return nil, fmt.Errorf("getting models: %w", err)
	}

	channelAccountStore := store.NewChannelAccountModel(dataModels.DB)

	channelAccountService, err := services.NewChannelAccountService(ctx, services.ChannelAccountServiceOptions{
		DB:                                 dataModels.DB,
		RPCService:                         rpcService,
		BaseFee:                            deps.BaseFee,
		DistributionAccountSignatureClient: deps.DistributionAccountSignatureClient,
		ChannelAccountStore:                channelAccountStore,
		PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
		EncryptionPassphrase:               deps.EncryptionPassphrase,
	})
	if err != nil {
		return nil, fmt.Errorf("creating channel account service: %w", err)
	}
	return channelAccountService, nil
}

func createPaymentService(models *data.Models, serverBaseURL string) (services.PaymentService, error) {
	paymentService, err := services.NewPaymentService(models, serverBaseURL)
	if err != nil {
		return nil, fmt.Errorf("creating payment service: %w", err)
	}
	return paymentService, nil
}

func createTransactionService(deps ServiceDependencies, rpcService services.RPCService) (txservices.TransactionService, error) {
	dbProvider := deps.DatabaseProvider.(*databaseProvider)
	channelAccountStore := store.NewChannelAccountModel(dbProvider.connectionPool)

	transactionService, err := txservices.NewTransactionService(txservices.TransactionServiceOptions{
		DB:                                 dbProvider.connectionPool,
		DistributionAccountSignatureClient: deps.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      deps.ChannelAccountSignatureClient,
		ChannelAccountStore:                channelAccountStore,
		RPCService:                         rpcService,
		BaseFee:                            deps.BaseFee,
	})
	if err != nil {
		return nil, fmt.Errorf("creating transaction service: %w", err)
	}
	return transactionService, nil
}

func extractHostname(serverBaseURL string) (string, error) {
	parsedURL, err := url.ParseRequestURI(serverBaseURL)
	if err != nil {
		return "", fmt.Errorf("parsing server base URL: %w", err)
	}
	return parsedURL.Hostname(), nil
}

