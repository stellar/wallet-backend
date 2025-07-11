package middleware

import (
	"context"
	"net/http"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/serve/graphql/dataloaders"
)

type ctxKey string

const (
	LoadersKey = ctxKey("dataloaders")
)

func DataloaderMiddleware(models *data.Models) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			loaders := dataloaders.NewDataloaders(models)
			ctx := context.WithValue(r.Context(), LoadersKey, loaders)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
