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

type tokenContractStore struct {
	cache *cache.Cache
	db    *data.ContractModel
}

func NewTokenContractStore(dbModel *data.ContractModel) (TokenContractStore, error) {
	store := &tokenContractStore{
		cache: cache.New(defaultExpiration, defaultExpiration),
		db:    dbModel,
	}

	// Populate cache with existing contracts
	contracts, err := store.db.GetAll(context.Background())
	if err != nil {
		return nil, fmt.Errorf("getting all contracts: %w", err)
	}

	if len(contracts) > 0 {
		for _, contract := range contracts {
			store.set(contract.ID, contract.Name, contract.Symbol)
		}
	}

	return store, nil
}

func (s *tokenContractStore) UpsertWithTx(ctx context.Context, contractID string, name string, symbol string) error {
	var contract *data.Contract

	err := db.RunInTransaction(ctx, s.db.DB, nil, func(tx db.Transaction) error {
		// Check if contract exists within the transaction
		existing, err := s.db.GetByID(ctx, contractID)
		if err != nil && !isNoRowsError(err) {
			return fmt.Errorf("checking existing contract: %w", err)
		}

		if existing != nil && (existing.Name != name || existing.Symbol != symbol) {
			// Update existing
			existing.Name = name
			existing.Symbol = symbol
			return s.db.Update(ctx, tx, existing)
		} else {
			// Insert new
			contract = &data.Contract{
				ID:     contractID,
				Name:   name,
				Symbol: symbol,
			}
			return s.db.Insert(ctx, tx, contract)
		}
	})
	if err != nil {
		return fmt.Errorf("upserting contract in db: %w", err)
	}

	// Update cache after successful database operation
	s.set(contractID, name, symbol)
	return nil
}

func (s *tokenContractStore) Name(ctx context.Context, contractID string) (string, error) {
	contractData, err := s.getContractData(contractID)
	if err != nil {
		return "", err
	}

	return contractData[contractNameKey], nil
}

func (s *tokenContractStore) Symbol(ctx context.Context, contractID string) (string, error) {
	contractData, err := s.getContractData(contractID)
	if err != nil {
		return "", err
	}

	return contractData[contractSymbolKey], nil
}

func (s *tokenContractStore) Exists(ctx context.Context, contractID string) bool {
	_, found := s.cache.Get(contractID)
	return found
}

func (s *tokenContractStore) getContractData(contractID string) (map[string]string, error) {
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

func (s *tokenContractStore) set(contractID, name, symbol string) {
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
