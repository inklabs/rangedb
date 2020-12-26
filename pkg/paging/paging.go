package paging

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
)

const (
	DefaultItemsPerPage = 10
	DefaultPage         = 1
	MaxItemsPerPage     = 1000
)

// Pagination contains page information for building pagination Links.
type Pagination struct {
	ItemsPerPage int
	Page         int
}

// Links contains previous/next URL links for pagination.
type Links struct {
	Previous string
	Next     string
}

// NewPagination constructs a Pagination object.
func NewPagination(itemsPerPage, page int) Pagination {
	if itemsPerPage <= 0 {
		itemsPerPage = DefaultItemsPerPage
	}

	if itemsPerPage > MaxItemsPerPage {
		itemsPerPage = MaxItemsPerPage
	}

	if page <= 0 {
		page = DefaultPage
	}

	return Pagination{
		ItemsPerPage: itemsPerPage,
		Page:         page,
	}
}

// NewPaginationFromQuery constructs a Pagination from a URL query.
func NewPaginationFromQuery(values url.Values) Pagination {
	itemsPerPage := values.Get("itemsPerPage")
	page := values.Get("page")

	return NewPaginationFromString(itemsPerPage, page)
}

// NewPaginationFromString constructs a Pagination from string input.
func NewPaginationFromString(itemsPerPageInput, pageInput string) Pagination {
	itemsPerPage, err := strconv.Atoi(itemsPerPageInput)
	if err != nil {
		itemsPerPage = DefaultItemsPerPage
	}

	page, err := strconv.Atoi(pageInput)
	if err != nil {
		page = DefaultPage
	}

	return NewPagination(itemsPerPage, page)
}

// Links returns the previous/next links.
func (p Pagination) Links(baseURI string, totalRecords uint64) Links {
	previous := ""
	next := ""

	if p.Page > 1 {
		previous = fmt.Sprintf("%s?itemsPerPage=%d&page=%d", baseURI, p.ItemsPerPage, p.Page-1)
	}

	totalPages := int(math.Ceil(float64(totalRecords) / float64(p.ItemsPerPage)))
	if p.Page < totalPages {
		next = fmt.Sprintf("%s?itemsPerPage=%d&page=%d", baseURI, p.ItemsPerPage, p.Page+1)
	}

	return Links{
		Previous: previous,
		Next:     next,
	}
}

// FirstRecordPosition returns the first record position.
func (p Pagination) FirstRecordPosition() int {
	return (p.Page - 1) * p.ItemsPerPage
}
