package services

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
)

type PaymentService interface {
	GetPaymentsPaginated(ctx context.Context, address string, beforeID, afterID string, sort data.SortOrder, limit int) ([]data.Payment, entities.Pagination, error)
}

var _ PaymentService = (*paymentService)(nil)

type paymentService struct {
	models        *data.Models
	serverBaseURL string
}

func NewPaymentService(models *data.Models, serverBaseURL string) (*paymentService, error) {
	if models == nil {
		return nil, errors.New("models cannot be nil")
	}

	if _, err := url.ParseRequestURI(serverBaseURL); err != nil {
		return nil, fmt.Errorf("invalid URL %s: %w", serverBaseURL, err)
	}

	return &paymentService{
		models:        models,
		serverBaseURL: serverBaseURL,
	}, nil
}

func (s *paymentService) GetPaymentsPaginated(ctx context.Context, address string, beforeID, afterID string, sort data.SortOrder, limit int) ([]data.Payment, entities.Pagination, error) {
	payments, prevExists, nextExists, err := s.models.Payments.GetPaymentsPaginated(ctx, address, beforeID, afterID, sort, limit)
	if err != nil {
		return nil, entities.Pagination{}, fmt.Errorf("getting payments: %w", err)
	}

	self, prev, next := "", "", ""
	self, err = buildURL(s.serverBaseURL, address, beforeID, afterID, sort, limit)
	if err != nil {
		return nil, entities.Pagination{}, fmt.Errorf("building self link: %w", err)
	}

	if prevExists {
		firstElementID := data.FirstPaymentOperationID(payments)
		prev, err = buildURL(s.serverBaseURL, address, firstElementID, "", sort, limit)
		if err != nil {
			return nil, entities.Pagination{}, fmt.Errorf("building prev link: %w", err)
		}
	}

	if nextExists {
		lastElementID := data.LastPaymentOperationID(payments)
		next, err = buildURL(s.serverBaseURL, address, "", lastElementID, sort, limit)
		if err != nil {
			return nil, entities.Pagination{}, fmt.Errorf("building next link: %w", err)
		}
	}

	pagination := entities.Pagination{
		Links: entities.PaginationLinks{
			Self: self,
			Prev: prev,
			Next: next,
		},
	}
	return payments, pagination, nil
}

func buildURL(baseURL string, address string, beforeID, afterID string, sort data.SortOrder, limit int) (string, error) {
	url, err := url.ParseRequestURI(baseURL)
	if err != nil {
		return "", fmt.Errorf("parsing base URL: %s: %w", baseURL, err)
	}

	values := url.Query()
	values.Add("sort", string(sort))
	values.Add("limit", strconv.Itoa(limit))
	if address != "" {
		values.Add("address", string(address))
	}
	if beforeID != "" {
		values.Add("beforeId", beforeID)
	}
	if afterID != "" {
		values.Add("afterId", afterID)
	}
	url.RawQuery = values.Encode()

	return url.String(), nil
}
