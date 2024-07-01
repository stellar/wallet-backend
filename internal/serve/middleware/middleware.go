package middleware

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/serve/auth"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
)

const MaxBodySize int64 = 10_240 // 10kb

func SignatureMiddleware(signatureVerifier auth.SignatureVerifier) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			sig := req.Header.Get("Signature")
			if sig == "" {
				sig = req.Header.Get("X-Stellar-Signature")
				if sig == "" {
					httperror.Unauthorized("", nil).Render(rw)
					return
				}
			}

			ctx := req.Context()

			reqBody, err := io.ReadAll(io.LimitReader(req.Body, MaxBodySize))
			if err != nil {
				err = fmt.Errorf("reading request body: %w", err)
				httperror.InternalServerError(ctx, "", err, nil).Render(rw)
				return
			}

			err = signatureVerifier.VerifySignature(ctx, sig, reqBody)
			if err != nil {
				err = fmt.Errorf("checking request signature: %w", err)
				log.Ctx(ctx).Error(err)
				httperror.Unauthorized("", nil).Render(rw)
				return
			}

			req.Body = io.NopCloser(bytes.NewReader(reqBody))
			next.ServeHTTP(rw, req)
		})
	}
}

// RecoverHandler is a middleware that recovers from panics and logs the error.
func RecoverHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("panic: %v", r)
			}

			// No need to recover when the client has disconnected:
			if errors.Is(err, http.ErrAbortHandler) {
				panic(err)
			}

			ctx := req.Context()
			log.Ctx(ctx).WithStack(err).Error(err)
			httperror.InternalServerError(ctx, "", err, nil).Render(rw)
		}()

		next.ServeHTTP(rw, req)
	})
}
