# GraphQL API Architecture

## Request Flow

<!-- TODO: Read internal/serve/serve.go for the full middleware chain.
     Create ASCII diagram:
       Client HTTP Request
         → Chi Router
         → Auth Middleware (JWT/Ed25519)
         → Metrics Middleware (Prometheus)
         → Recovery Middleware
         → DataLoader Middleware (request-scoped loaders)
         → gqlgen Handler
         → Resolver
         → Service Layer
         → DataLoader / DB
         → Response -->

## Schema Organization

<!-- TODO: List all .graphqls files from internal/serve/graphql/schema/ with a 1-line description of each.
     For example:
     - schema.graphqls — Root Query and Mutation definitions
     - transaction.graphqls — Transaction type and queries
     - ... etc.
     Note which types are shared across schemas. -->

## Resolver Architecture

<!-- TODO: Read internal/serve/graphql/resolvers/ for the follow-schema layout.
     Document:
     - How resolver files map to schema files (1:1 naming)
     - The resolver struct and its dependencies
     - How resolvers call into the service layer
     - Error handling patterns in resolvers -->

## DataLoader Pattern

<!-- TODO: Read internal/serve/graphql/dataloaders/loaders.go.
     Document:
     - All loader types and what they batch
     - How loaders are injected via middleware (request-scoped)
     - The loader key → batch fetch → distribute pattern
     - Example: loading operations for N transactions in 1 query instead of N queries -->

## Mutation Flow

<!-- TODO: Trace the transaction submission flow:
     GraphQL Mutation → TransactionService.BuildAndSign() → FeeBumpService → RPCService.Submit()
     Document:
     - How channel accounts are acquired and released
     - Sequence number management
     - Fee bumping strategy
     - RPC submission and status polling -->
