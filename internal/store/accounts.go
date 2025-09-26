package store

import (
	"context"
	"fmt"

	set "github.com/deckarep/golang-set/v2"

	"github.com/stellar/wallet-backend/internal/data"
)

type accountsStore struct {
	cache set.Set[string]
	db    *data.AccountModel
}

func NewAccountsStore(dbModel *data.AccountModel) (AccountsStore, error) {
	store := &accountsStore{
		cache: set.NewSet[string](),
		db:    dbModel,
	}

	// Populate cache with existing accounts. This also handles the scenarios where we
	// register/deregister accounts in the db and the wallet backend service terminated before we update the cache.
	// This cache hydration helps keeps the cache in sync with the db even during restarts.
	accounts, err := store.db.GetAll(context.Background())
	if err != nil {
		return nil, fmt.Errorf("getting all accounts: %w", err)
	}

	if len(accounts) > 0 {
		for _, account := range accounts {
			store.Add(account)
		}
	}

	return store, nil
}

func (s *accountsStore) Exists(ctx context.Context, accountID string) bool {
	return s.cache.Contains(accountID)
}

func (s *accountsStore) Add(accountID string) {
	s.cache.Add(accountID)
}

func (s *accountsStore) Remove(accountID string) {
	s.cache.Remove(accountID)
}
