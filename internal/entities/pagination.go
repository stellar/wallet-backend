package entities

type PaginationLinks struct {
	Self string `json:"self"`
	Prev string `json:"prev"`
	Next string `json:"next"`
}

type Pagination struct {
	Links PaginationLinks `json:"_links"`
}
