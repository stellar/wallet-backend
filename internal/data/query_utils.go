package data

type SortOrder string

const (
	ASC  SortOrder = "ASC"
	DESC SortOrder = "DESC"
)

func (o SortOrder) IsValid() bool {
	return o == ASC || o == DESC
}
