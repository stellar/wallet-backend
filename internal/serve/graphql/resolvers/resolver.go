// GraphQL resolvers package - implements resolver functions for GraphQL schema
// This package contains the business logic for handling GraphQL queries and field resolution
package resolvers

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.
// This is the main resolver struct that gqlgen uses to resolve GraphQL queries.

import (
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/services"
)

// Resolver is the main resolver struct for gqlgen
// It holds dependencies needed by all resolver functions
// gqlgen will embed this struct in generated resolver interfaces
type Resolver struct {
	// models provides access to data layer for database operations
	// This follows dependency injection pattern - resolvers don't create their own DB connections
	models *data.Models
	// accountService provides account management operations
	accountService services.AccountService
	// accountSponsorshipService provides transaction sponsorship operations
	accountSponsorshipService services.AccountSponsorshipService
}

// NewResolver creates a new resolver instance with required dependencies
// This constructor is called during server startup to initialize the resolver
// Dependencies are injected here and available to all resolver functions.
func NewResolver(models *data.Models, accountService services.AccountService, accountSponsorshipService services.AccountSponsorshipService) *Resolver {
	return &Resolver{
		models:                    models,
		accountService:            accountService,
		accountSponsorshipService: accountSponsorshipService,
	}
}
