package entities

type PaginationLinks struct {
	Self string `json:"self"`
	Prev string `json:"next"`
	Next string `json:"prev"`
}

type Pagination struct {
	Links PaginationLinks `json:"_links"`
}
