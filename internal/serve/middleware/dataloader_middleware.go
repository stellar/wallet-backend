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
			// Dataloaders are created per-request to avoid data sharing between requests.
			// This ensures each request has a fresh view of the data. This also helps prevent
			// inconsistent data view across horizontally scaled services.
			// More info about this here: https://github.com/graphql/dataloader/issues/62#issue-193854091
			loaders := dataloaders.NewDataloaders(models)
			ctx := context.WithValue(r.Context(), LoadersKey, loaders)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
