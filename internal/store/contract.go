// Package store provides storage interfaces and implementations for wallet-backend.
// This file contains the in-memory implementation of the contract store.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
)

const (
	contractNameKey   = "name"
	contractSymbolKey = "symbol"
	defaultExpiration = 30 * 24 * time.Hour
)

type contractStore struct {
	cache *cache.Cache
	db    *data.ContractModel
}

func NewContractStore(dbModel *data.ContractModel) ContractStore {
	store := &contractStore{
		cache: cache.New(defaultExpiration, defaultExpiration),
		db:    dbModel,
	}

	// Populate cache with existing contracts
	contracts, err := store.db.GetAll(context.Background())
	if err == nil && len(contracts) > 0 {
		for _, contract := range contracts {
			store.set(contract.ID, contract.Name, contract.Symbol)
		}
	}

	return store
}

func (s *contractStore) UpsertWithTx(ctx context.Context, contractID string, name string, symbol string) error {
	// Check if contract exists in database
	existing, err := s.db.GetByID(ctx, contractID)
	if err != nil && !isNoRowsError(err) {
		return fmt.Errorf("checking existing contract: %w", err)
	}

	if existing.ID != "" {
		// Update existing contract
		existing.Name = name
		existing.Symbol = symbol
		err = db.RunInTransaction(ctx, s.db.DB, nil, func(tx db.Transaction) error {
			return s.db.Update(ctx, tx, existing)
		})
		if err != nil {
			return fmt.Errorf("updating contract in database: %w", err)
		}
	} else {
		// Insert new contract
		contract := &data.Contract{
			ID:     contractID,
			Name:   name,
			Symbol: symbol,
		}
		err = db.RunInTransaction(ctx, s.db.DB, nil, func(tx db.Transaction) error {
			return s.db.Insert(ctx, tx, contract)
		})
		if err != nil {
			return fmt.Errorf("inserting contract in database: %w", err)
		}
	}

	// Update cache after successful database operation
	s.set(contractID, name, symbol)
	return nil
}

func (s *contractStore) Name(ctx context.Context, contractID string) (string, error) {
	contractData, err := s.getContractData(contractID)
	if err != nil {
		return "", err
	}

	return contractData[contractNameKey], nil
}

func (s *contractStore) Symbol(ctx context.Context, contractID string) (string, error) {
	contractData, err := s.getContractData(contractID)
	if err != nil {
		return "", err
	}

	return contractData[contractSymbolKey], nil
}

func (s *contractStore) Exists(ctx context.Context, contractID string) (bool, error) {
	_, found := s.cache.Get(contractID)
	return found, nil
}

func (s *contractStore) getContractData(contractID string) (map[string]string, error) {
	data, found := s.cache.Get(contractID)
	if !found {
		return nil, fmt.Errorf("getting contract data: contract not found")
	}

	// Since Go is statically typed, and cache values can be anything, type
	// assertion is needed when values are being passed to functions that don't
	// take arbitrary types, (i.e. interface{}).
	contractData, ok := data.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("getting contract data: invalid data type")
	}

	return contractData, nil
}

func (s *contractStore) set(contractID, name, symbol string) {
	contractData := map[string]string{
		contractNameKey:   name,
		contractSymbolKey: symbol,
	}
	s.cache.Set(contractID, contractData, defaultExpiration)
}

func isNoRowsError(err error) bool {
	// Check if this is a "no rows" error
	return errors.Is(err, sql.ErrNoRows)
}
