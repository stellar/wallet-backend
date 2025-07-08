package graphql

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

import (
	"github.com/stellar/wallet-backend/internal/data"
)

type Resolver struct {
	models *data.Models
}

func NewResolver(models *data.Models) *Resolver {
	return &Resolver{models: models}
}
