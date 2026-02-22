# Services Architecture

## Service Pattern

All services follow a consistent construction pattern:

<!-- TODO: Read any service file (e.g., internal/services/transaction_service.go) and show the pattern:
     type FooServiceOptions struct {
         DB            *sql.DB
         OtherService  BarServiceInterface
         // ... dependencies
     }

     func (o *FooServiceOptions) ValidateOptions() error {
         // validate all required dependencies are set
     }

     type FooService struct {
         // same fields as Options
     }

     func NewFooService(opts FooServiceOptions) (*FooService, error) {
         if err := opts.ValidateOptions(); err != nil {
             return nil, err
         }
         return &FooService{...}, nil
     }

     Document how this pattern enables testability and explicit dependency declaration. -->

## Service Inventory

<!-- TODO: Read internal/services/*.go and list each service with a 1-line description:
     - TransactionService — builds, signs, and submits Stellar transactions
     - RPCService — communicates with Stellar RPC endpoint
     - FeeBumpService — wraps transactions with fee bumps
     - IngestService — coordinates live and backfill ingestion
     - ChannelAccountService — manages channel account lifecycle
     - ... (list all) -->

## Dependency Graph

<!-- TODO: Create ASCII diagram showing which services depend on which:
     TransactionService
       ├── RPCService
       ├── FeeBumpService
       ├── SignatureClient
       └── data.Models

     IngestService
       ├── RPCService
       └── data.Models

     ... etc.

     Read internal/serve/serve.go:initHandlerDeps for the full wiring. -->
