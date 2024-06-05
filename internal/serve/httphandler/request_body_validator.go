package httphandler

import (
	"context"

	"github.com/go-playground/validator/v10"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/validators"
)

func ValidateRequestBody[T any](ctx context.Context, reqBody T) *httperror.ErrorResponse {
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
