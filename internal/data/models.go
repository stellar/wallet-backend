package data

import (
	"errors"
	"wallet-backend/internal/db"
)

type Models struct {
	Payments *PaymentModel
}

func NewModels(db db.DBConnectionPool) (*Models, error) {
	if db == nil {
		return nil, errors.New("DBConnectionPool must be initialized")
	}

	return &Models{
		Payments: &PaymentModel{db: db},
	}, nil
}
