// Package store provides storage interfaces and implementations for wallet-backend.
// This file contains the in-memory implementation of the contract store.
package store

import (
	"context"
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
			contractData := map[string]string{
				contractNameKey:   contract.Name,
				contractSymbolKey: contract.Symbol,
			}
			store.cache.Add(contract.ID, contractData, cache.DefaultExpiration)
		}
	}

	return store
}

func (s *contractStore) InsertWithTx(ctx context.Context, tx db.Transaction, contractID string, name string, symbol string) error {
	contract := &data.Contract{
		ID:     contractID,
		Name:   name,
		Symbol: symbol,
	}
	
	err := s.db.Insert(ctx, tx, contract)
	if err != nil {
		return fmt.Errorf("inserting contract in database: %w", err)
	}
	
	// Only update cache after successful database operation
	contractData := map[string]string{
		contractNameKey:   name,
		contractSymbolKey: symbol,
	}
	s.cache.Add(contractID, contractData, defaultExpiration)
	
	return nil
}

func (s *contractStore) UpdateWithTx(ctx context.Context, tx db.Transaction, contractID string, name string, symbol string) error {
	contract, err := s.db.GetByID(ctx, contractID)
	if err != nil {
		return fmt.Errorf("getting contract from database: %w", err)
	}

	if contract == nil {
		return fmt.Errorf("contract not found")
	}

	contract.Name = name
	contract.Symbol = symbol
	err = s.db.Update(ctx, tx, contract)
	if err != nil {
		return fmt.Errorf("updating contract in database: %w", err)
	}

	contractData := map[string]string{
		contractNameKey:   name,
		contractSymbolKey: symbol,
	}
	s.cache.Set(contractID, contractData, defaultExpiration)
	return nil
}

func (s *contractStore) Name(ctx context.Context, contractID string) (string, error) {
	contractData, err := s.getContractData(contractID)
	if err != nil {
		return "", fmt.Errorf("getting contract data: %w", err)
	}

	name, exists := contractData[contractNameKey]
	if !exists {
		return "", fmt.Errorf("getting contract name: name field not found")
	}

	return name, nil
}

func (s *contractStore) Symbol(ctx context.Context, contractID string) (string, error) {
	contractData, err := s.getContractData(contractID)
	if err != nil {
		return "", fmt.Errorf("getting contract data: %w", err)
	}

	symbol, exists := contractData[contractSymbolKey]
	if !exists {
		return "", fmt.Errorf("getting contract symbol: symbol field not found")
	}

	return symbol, nil
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
