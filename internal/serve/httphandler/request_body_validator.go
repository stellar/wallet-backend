package httphandler

import (
	"context"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/stellar/go/support/http/httpdecode"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/validators"
)

func DecodeJSONAndValidate(ctx context.Context, req *http.Request, reqBody interface{}) *httperror.ErrorResponse {
	err := httpdecode.DecodeJSON(req, reqBody)
	if err != nil {
		return httperror.BadRequest("Invalid request body.", nil)
	}

	return ValidateRequestBody(ctx, reqBody)
}

func ValidateRequestBody(ctx context.Context, reqBody interface{}) *httperror.ErrorResponse {
	val := validators.NewValidator()
	if err := val.StructCtx(ctx, reqBody); err != nil {
		if vErrs, ok := err.(validator.ValidationErrors); ok {
			extras := validators.ParseValidationError(vErrs)
			return httperror.BadRequest("Validation error.", extras)
		}
		return httperror.InternalServerError(ctx, "", err, nil)
	}
	return nil
}
