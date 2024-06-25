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

	return ValidateRequestParams(ctx, reqBody)
}

func DecodeQueryAndValidate(ctx context.Context, req *http.Request, reqQuery interface{}) *httperror.ErrorResponse {
	err := httpdecode.DecodeQuery(req, reqQuery)
	if err != nil {
		return httperror.BadRequest("Invalid request URL params.", nil)
	}

	return ValidateRequestParams(ctx, reqQuery)
}

func ValidateRequestParams(ctx context.Context, reqParams interface{}) *httperror.ErrorResponse {
	val := validators.NewValidator()
	if err := val.StructCtx(ctx, reqParams); err != nil {
		if vErrs, ok := err.(validator.ValidationErrors); ok {
			extras := validators.ParseValidationError(vErrs)
			return httperror.BadRequest("Validation error.", extras)
		}
		return httperror.InternalServerError(ctx, "", err, nil)
	}
	return nil
}
