package rangedb

import (
	"strconv"
)

const (
	DefaultItemsPerPage = 20
	DefaultPage         = 1
	MaxItemsPerPage     = 1000
)

type Pagination struct {
	ItemsPerPage int
	Page         int
}

func DefaultPagination() Pagination {
	return NewPagination(DefaultItemsPerPage, DefaultPage)
}

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
