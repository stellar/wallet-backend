package services

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
)

type PaymentService struct {
	Models        *data.Models
	ServerBaseURL string
}

func (s *PaymentService) GetPaymentsPaginated(ctx context.Context, address string, beforeID, afterID int64, sort data.SortOrder, limit int) ([]data.Payment, entities.Pagination, error) {
	payments, prevExists, nextExists, err := s.Models.Payments.GetPaymentsPaginated(ctx, address, beforeID, afterID, sort, limit)
	if err != nil {
		return nil, entities.Pagination{}, fmt.Errorf("getting payments: %w", err)
	}

	self, prev, next := "", "", ""
	self, err = buildURL(s.ServerBaseURL, address, beforeID, afterID, sort, limit)
	if err != nil {
		return nil, entities.Pagination{}, fmt.Errorf("building self link: %w", err)
	}

	if prevExists {
		firstElementID := data.FirstPaymentOperationID(payments)
		prev, err = buildURL(s.ServerBaseURL, address, firstElementID, 0, sort, limit)
		if err != nil {
			return nil, entities.Pagination{}, fmt.Errorf("building prev link: %w", err)
		}
	}

	if nextExists {
		lastElementID := data.LastPaymentOperationID(payments)
		next, err = buildURL(s.ServerBaseURL, address, 0, lastElementID, sort, limit)
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

func buildURL(baseURL string, address string, beforeID, afterID int64, sort data.SortOrder, limit int) (string, error) {
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
	if beforeID != 0 {
		values.Add("beforeId", strconv.FormatInt(beforeID, 10))
	}
	if afterID != 0 {
		values.Add("afterId", strconv.FormatInt(afterID, 10))
	}
	url.RawQuery = values.Encode()

	return url.String(), nil
}
