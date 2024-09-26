package serve

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/clients/horizonclient"
	supporthttp "github.com/stellar/go/support/http"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/render/health"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/serve/auth"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/serve/httphandler"
	"github.com/stellar/wallet-backend/internal/serve/middleware"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	signingutils "github.com/stellar/wallet-backend/internal/signing/utils"
	"github.com/stellar/wallet-backend/internal/tss"
	tsschannel "github.com/stellar/wallet-backend/internal/tss/channels"
	tssrouter "github.com/stellar/wallet-backend/internal/tss/router"
	tssservices "github.com/stellar/wallet-backend/internal/tss/services"
	tssstore "github.com/stellar/wallet-backend/internal/tss/store"
)

// NOTE: perhaps move this to a environment variable.
var blockedOperationTypes = []xdr.OperationType{
	xdr.OperationTypeInvokeHostFunction,
	xdr.OperationTypeExtendFootprintTtl,
	xdr.OperationTypeRestoreFootprint,
	xdr.OperationTypeLiquidityPoolWithdraw,
	xdr.OperationTypeLiquidityPoolDeposit,
	xdr.OperationTypeClawbackClaimableBalance,
	xdr.OperationTypeClawback,
	xdr.OperationTypeClaimClaimableBalance,
	xdr.OperationTypeCreateClaimableBalance,
	xdr.OperationTypeInflation,
}

type Configs struct {
	Port                    int
	DatabaseURL             string
	ServerBaseURL           string
	WalletSigningKey        string
	LogLevel                logrus.Level
	EncryptionPassphrase    string
	NumberOfChannelAccounts int

	// Horizon
	SupportedAssets                    []entities.Asset
	NetworkPassphrase                  string
	MaxSponsoredBaseReserves           int
	BaseFee                            int
	HorizonClientURL                   string
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	// TSS
	RPCURL                                               string
	RPCCallerServiceChannelBufferSize                    int
	RPCCallerServiceChannelMaxWorkers                    int
	ErrorHandlerServiceJitterChannelBufferSize           int
	ErrorHandlerServiceJitterChannelMaxWorkers           int
	ErrorHandlerServiceNonJitterChannelBufferSize        int
	ErrorHandlerServiceNonJitterChannelMaxWorkers        int
	ErrorHandlerServiceJitterChannelMinWaitBtwnRetriesMS int
	ErrorHandlerServiceNonJitterChannelWaitBtwnRetriesMS int
	ErrorHandlerServiceJitterChannelMaxRetries           int
	ErrorHandlerServiceNonJitterChannelMaxRetries        int
	// Error Tracker
	AppTracker apptracker.AppTracker
}

type handlerDeps struct {
	Models            *data.Models
	Port              int
	DatabaseURL       string
	SignatureVerifier auth.SignatureVerifier
	SupportedAssets   []entities.Asset

	// Services
	AccountService            services.AccountService
	AccountSponsorshipService services.AccountSponsorshipService
	PaymentService            services.PaymentService
	// TSS
	RPCCallerServiceChannel tss.Channel
	TSSRouter               tssrouter.Router
	AppTracker              apptracker.AppTracker
}

func Serve(cfg Configs) error {
	deps, err := initHandlerDeps(cfg)
	if err != nil {
		return fmt.Errorf("setting up handler dependencies: %w", err)
	}

	addr := fmt.Sprintf(":%d", cfg.Port)
	supporthttp.Run(supporthttp.Config{
		ListenAddr: addr,
		Handler:    handler(deps),
		OnStarting: func() {
			log.Infof("Starting Wallet Backend server on port %d", cfg.Port)
		},
		OnStopping: func() {
			log.Info("Stopping Wallet Backend server")
			deps.ErrorHandlerServiceJitterChannel.Stop()
			deps.ErrorHandlerServiceNonJitterChannel.Stop()
			deps.RPCCallerServiceChannel.Stop()
		},
	})

	return nil
}

func initHandlerDeps(cfg Configs) (handlerDeps, error) {
	dbConnectionPool, err := db.OpenDBConnectionPool(cfg.DatabaseURL)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("connecting to the database: %w", err)
	}
	models, err := data.NewModels(dbConnectionPool)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("creating models for Serve: %w", err)
	}

	signatureVerifier, err := auth.NewStellarSignatureVerifier(cfg.ServerBaseURL, cfg.WalletSigningKey)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating stellar signature verifier: %w", err)
	}

	horizonClient := horizonclient.Client{
		HorizonURL: cfg.HorizonClientURL,
		HTTP:       &http.Client{Timeout: 40 * time.Second},
	}

	accountService, err := services.NewAccountService(models)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating account service: %w", err)
	}

	accountSponsorshipService, err := services.NewAccountSponsorshipService(services.AccountSponsorshipServiceOptions{
		DistributionAccountSignatureClient: cfg.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      cfg.ChannelAccountSignatureClient,
		HorizonClient:                      &horizonClient,
		MaxSponsoredBaseReserves:           cfg.MaxSponsoredBaseReserves,
		BaseFee:                            int64(cfg.BaseFee),
		Models:                             models,
		BlockedOperationsTypes:             blockedOperationTypes,
	})
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating account sponsorship service: %w", err)
	}

	paymentService, err := services.NewPaymentService(models, cfg.ServerBaseURL)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating payment service: %w", err)
	}

	channelAccountService, err := services.NewChannelAccountService(services.ChannelAccountServiceOptions{
		DB:                                 dbConnectionPool,
		HorizonClient:                      &horizonClient,
		BaseFee:                            int64(cfg.BaseFee),
		DistributionAccountSignatureClient: cfg.DistributionAccountSignatureClient,
		ChannelAccountStore:                store.NewChannelAccountModel(dbConnectionPool),
		PrivateKeyEncrypter:                &signingutils.DefaultPrivateKeyEncrypter{},
		EncryptionPassphrase:               cfg.EncryptionPassphrase,
	})
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating channel account service: %w", err)
	}
	go ensureChannelAccounts(channelAccountService, int64(cfg.NumberOfChannelAccounts))

	// TSS
	txServiceOpts := tssservices.TransactionServiceOptions{
		DistributionAccountSignatureClient: cfg.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      cfg.ChannelAccountSignatureClient,
		HorizonClient:                      &horizonClient,
		BaseFee:                            int64(cfg.BaseFee), // Reuse horizon base fee for RPC??
	}
	tssTxService, err := tssservices.NewTransactionService(txServiceOpts)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating tss transaction service: %w", err)
	}
	httpClient := http.Client{Timeout: time.Duration(30 * time.Second)}
	rpcService, err := services.NewRPCService(cfg.RPCURL, &httpClient)
	if err != nil {
		return handlerDeps{}, fmt.Errorf("instantiating rpc service: %w", err)
	}

	store := tssstore.NewStore(dbConnectionPool)
	txManager := tssservices.NewTransactionManager(tssservices.TransactionManagerConfigs{
		TxService:  tssTxService,
		RPCService: rpcService,
		Store:      store,
	})
	tssChannelConfigs := tsschannel.RPCCallerChannelConfigs{
		TxManager:     txManager,
		Store:         store,
		MaxBufferSize: cfg.RPCCallerServiceChannelBufferSize,
		MaxWorkers:    cfg.RPCCallerServiceChannelMaxWorkers,
	}
	rpcCallerServiceChannel := tsschannel.NewRPCCallerChannel(tssChannelConfigs)

	router := tssrouter.NewRouter(tssrouter.RouterConfigs{
		RPCCallerChannel:      rpcCallerServiceChannel,
		ErrorJitterChannel:    nil,
		ErrorNonJitterChannel: nil,
		WebhookChannel:        nil,
	})

	rpcCallerServiceChannel.SetRouter(router)

	jitterChannelOpts := tsschannel.RPCErrorHandlerServiceJitterChannelConfigs{
		Store:                store,
		TxService:            tssTxService,
		MaxBufferSize:        cfg.ErrorHandlerServiceJitterChannelBufferSize,
		MaxWorkers:           cfg.ErrorHandlerServiceJitterChannelMaxWorkers,
		MaxRetries:           cfg.ErrorHandlerServiceJitterChannelMaxRetries,
		MinWaitBtwnRetriesMS: cfg.ErrorHandlerServiceJitterChannelMinWaitBtwnRetriesMS,
	}

	jitterChannel := tsschannel.NewErrorHandlerServiceJitterChannel(jitterChannelOpts)

	nonJitterChannelOpts := tsschannel.RPCErrorHandlerServiceNonJitterChannelConfigs{
		Store:             store,
		TxService:         tssTxService,
		MaxBufferSize:     cfg.ErrorHandlerServiceNonJitterChannelBufferSize,
		MaxWorkers:        cfg.ErrorHandlerServiceNonJitterChannelMaxWorkers,
		MaxRetries:        cfg.ErrorHandlerServiceNonJitterChannelMaxRetries,
		WaitBtwnRetriesMS: cfg.ErrorHandlerServiceNonJitterChannelWaitBtwnRetriesMS,
	}

	nonJitterChannel := tsschannel.NewErrorHandlerServiceNonJitterChannel(nonJitterChannelOpts)

	errHandlerService := tssservices.NewErrorHandlerService(tssservices.ErrorHandlerServiceConfigs{
		JitterChannel:    jitterChannel,
		NonJitterChannel: nonJitterChannel,
	})

	webhookHandlerService := tssservices.NewWebhookHandlerService(nil)

	router := tssrouter.NewRouter(tssrouter.RouterConfigs{
		ErrorHandlerService:   errHandlerService,
		WebhookHandlerService: webhookHandlerService,
	})

	jitterChannel.SetRouter(router)
	nonJitterChannel.SetRouter(router)
	rpcCallerServiceChannel.SetRouter(router)

	return handlerDeps{
		Models:                    models,
		SignatureVerifier:         signatureVerifier,
		SupportedAssets:           cfg.SupportedAssets,
		AccountService:            accountService,
		AccountSponsorshipService: accountSponsorshipService,
		PaymentService:            paymentService,
		AppTracker:                cfg.AppTracker,
		// TSS
		RPCCallerServiceChannel: rpcCallerServiceChannel,
		TSSRouter:               router,
	}, nil
}

func ensureChannelAccounts(channelAccountService services.ChannelAccountService, numberOfChannelAccounts int64) {
	ctx := context.Background()
	log.Ctx(ctx).Info("Ensuring the number of channel accounts in the database...")
	err := channelAccountService.EnsureChannelAccounts(ctx, numberOfChannelAccounts)
	if err != nil {
		log.Ctx(ctx).Errorf("error ensuring the number of channel accounts: %s", err.Error())
		return
	}
	log.Ctx(ctx).Infof("Ensured that at least %d channel accounts exist in the database", numberOfChannelAccounts)
}

func handler(deps handlerDeps) http.Handler {
	mux := supporthttp.NewAPIMux(log.DefaultLogger)
	mux.NotFound(httperror.ErrorHandler{Error: httperror.NotFound}.ServeHTTP)
	mux.MethodNotAllowed(httperror.ErrorHandler{Error: httperror.MethodNotAllowed}.ServeHTTP)
	mux.Use(middleware.RecoverHandler(deps.AppTracker))

	mux.Get("/health", health.PassHandler{}.ServeHTTP)

	// Authenticated routes
	mux.Group(func(r chi.Router) {
		r.Use(middleware.SignatureMiddleware(deps.SignatureVerifier, deps.AppTracker))

		r.Route("/accounts", func(r chi.Router) {
			handler := &httphandler.AccountHandler{
				AccountService:            deps.AccountService,
				AccountSponsorshipService: deps.AccountSponsorshipService,
				SupportedAssets:           deps.SupportedAssets,
				AppTracker:                deps.AppTracker,
			}

			r.Post("/{address}", handler.RegisterAccount)
			r.Delete("/{address}", handler.DeregisterAccount)
		})

		r.Route("/payments", func(r chi.Router) {
			handler := &httphandler.PaymentHandler{
				PaymentService: deps.PaymentService,
				AppTracker:     deps.AppTracker,
			}

			r.Get("/", handler.GetPayments)
		})

		r.Route("/tx", func(r chi.Router) {
			handler := &httphandler.AccountHandler{
				AccountService:            deps.AccountService,
				AccountSponsorshipService: deps.AccountSponsorshipService,
				SupportedAssets:           deps.SupportedAssets,
				AppTracker:                deps.AppTracker,
			}

			r.Post("/create-sponsored-account", handler.SponsorAccountCreation)
			r.Post("/create-fee-bump", handler.CreateFeeBumpTransaction)
		})
	})

	return mux
}
