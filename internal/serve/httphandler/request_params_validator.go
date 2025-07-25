package httphandler

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/stellar/go/support/http/httpdecode"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/validators"
)

func DecodeJSONAndValidate(ctx context.Context, req *http.Request, reqBody interface{}, appTracker apptracker.AppTracker) *httperror.ErrorResponse {
	err := httpdecode.DecodeJSON(req, reqBody)
	if err != nil {
		return httperror.BadRequest("Invalid request body.", nil)
	}

	return ValidateRequestParams(ctx, reqBody, appTracker)
}

func DecodeQueryAndValidate(ctx context.Context, req *http.Request, reqQuery interface{}, appTracker apptracker.AppTracker) *httperror.ErrorResponse {
	err := httpdecode.DecodeQuery(req, reqQuery)
	if err != nil {
		return httperror.BadRequest("Invalid request URL params.", nil)
	}

	return ValidateRequestParams(ctx, reqQuery, appTracker)
}

func ValidateRequestParams(ctx context.Context, reqParams interface{}, appTracker apptracker.AppTracker) *httperror.ErrorResponse {
	val, err := validators.NewValidator()
	if err != nil {
		return httperror.InternalServerError(ctx, "Internal error while creating a new validator.", err, nil, appTracker)
	}

	if err := val.StructCtx(ctx, reqParams); err != nil {
		var vErrs validator.ValidationErrors
		if ok := errors.As(err, &vErrs); ok {
			extras := validators.ParseValidationError(vErrs)
			return httperror.BadRequest("Validation error.", extras)
		}
		return httperror.InternalServerError(ctx, "", err, nil, appTracker)
	}
	return nil
}
