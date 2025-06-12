// Package store provides storage interfaces and implementations for wallet-backend.
// This file contains the in-memory implementation of the contract store.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
)

const (
	contractNameKey   = "name"
	contractSymbolKey = "symbol"
	defaultExpiration = 7 * 24 * time.Hour
)

type contractStore struct {
	cache *cache.Cache
}

func NewContractStore() ContractStore {
	// Create cache with default expiration of 24 hours and cleanup every 10 minutes
	return &contractStore{
		cache: cache.New(defaultExpiration, 10*time.Minute),
	}
}

func (s *contractStore) Set(ctx context.Context, contractID string, name string, symbol string) error {
	contractData := map[string]string{
		contractNameKey:   name,
		contractSymbolKey: symbol,
	}
	s.cache.Set(contractID, contractData, cache.DefaultExpiration)
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
