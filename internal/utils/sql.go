package utils

import (
	"database/sql"
	"time"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func SQLNullString(s string) sql.NullString {
	return sql.NullString{
		String: s,
		Valid:  s != "",
	}
}

func SQLNullTime(t time.Time) sql.NullTime {
	return sql.NullTime{
		Time:  t,
		Valid: !t.IsZero(),
	}
}

func NullAddressBytea(s string) types.NullAddressBytea {
	return types.NullAddressBytea{
		AddressBytea: types.AddressBytea(s),
		Valid:        s != "",
	}
}
