package services

import (
	"fmt"

	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/store"
)

type PoolPopulator interface {
	PopulatePools() error
}

type poolPopulator struct {
	Router router.Router
	Store  store.Store
}

func NewPoolPopulator(router router.Router, store store.Store) (*poolPopulator, error) {
	if router == nil {
		return nil, fmt.Errorf("router is nil")
	}
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	return &poolPopulator{
		Router: router,
		Store:  store,
	}, nil
}

func (p *poolPopulator) PopulatePools() error {
	/*
		1. Get all txns of status new
		2. get the latest try associated with them
		3. If the latest try is of status rpc fail or unmarshall error, check timebounds, and iff time bounds exceeded, try it again
	*/
	return nil

}
