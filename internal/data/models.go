package data

import (
	"errors"

	"github.com/stellar/wallet-backend/internal/db"
)

type Models struct {
	Payments *PaymentModel
	Account  *AccountModel
}

func NewModels(db db.ConnectionPool) (*Models, error) {
	if db == nil {
		return nil, errors.New("ConnectionPool must be initialized")
	}

	return &Models{
		Payments: &PaymentModel{DB: db},
		Account:  &AccountModel{DB: db},
	}, nil
}
