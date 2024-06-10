package middleware

import (
	"bytes"
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
