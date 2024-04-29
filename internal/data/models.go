package data

import (
	"errors"
	"wallet-backend/internal/db"
)

type Models struct {
}

func NewModels(db db.DBConnectionPool) (*Models, error) {
	if db == nil {
		return nil, errors.New("DBConnectionPool must be initialized")
	}

	return &Models{}, nil
}
