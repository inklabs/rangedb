package paging_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/paging"
)

func TestNewLinks_HasNextPage(t *testing.T) {
	// Given
	totalRecords := uint64(16)
	pagination := paging.NewPagination(10, 1)

	// When
	links := pagination.Links("/foo/bar", totalRecords)

	// Then
	assert.Equal(t, "", links.Previous)
	assert.Equal(t, "/foo/bar?itemsPerPage=10&page=2", links.Next)
}

func TestNewLinks_HasPreviousAndNextPage(t *testing.T) {
	// Given
	totalRecords := uint64(100)
	pagination := paging.NewPagination(10, 2)

	// When
	links := pagination.Links("/e/thing", totalRecords)

	// Then
	assert.Equal(t, "/e/thing?itemsPerPage=10&page=1", links.Previous)
	assert.Equal(t, "/e/thing?itemsPerPage=10&page=3", links.Next)
}

func TestNewLinks_HasPreviousAndNoNextPage(t *testing.T) {
	// Given
	totalRecords := uint64(20)
	pagination := paging.NewPagination(10, 2)

	// When
	links := pagination.Links("/e/thing", totalRecords)

	// Then
	assert.Equal(t, "/e/thing?itemsPerPage=10&page=1", links.Previous)
	assert.Equal(t, "", links.Next)
}

func TestNewPagination_ReturnsDefaultValueForItemsPerPage(t *testing.T) {
	// When
	pagination := paging.NewPagination(0, 0)

	// Then
	assert.Equal(t, paging.DefaultItemsPerPage, pagination.ItemsPerPage)
	assert.Equal(t, paging.DefaultPage, pagination.Page)
}

func TestNewPagination_ReturnsMaxValueForItemsPerPage(t *testing.T) {
	// When
	pagination := paging.NewPagination(paging.MaxItemsPerPage+1, 1)

	// Then
	assert.Equal(t, paging.MaxItemsPerPage, pagination.ItemsPerPage)
}

func TestNewPaginationFromQuery(t *testing.T) {
	// Given
	values := url.Values{}
	values.Set("itemsPerPage", "2")
	values.Set("page", "3")

	// When
	pagination := paging.NewPaginationFromQuery(values)

	// Then
	assert.Equal(t, 2, pagination.ItemsPerPage)
	assert.Equal(t, 3, pagination.Page)
}

func TestNewPaginationFromQuery_UsingDefaultValues(t *testing.T) {
	// Given
	values := url.Values{}

	// When
	pagination := paging.NewPaginationFromQuery(values)

	// Then
	assert.Equal(t, paging.DefaultItemsPerPage, pagination.ItemsPerPage)
	assert.Equal(t, paging.DefaultPage, pagination.Page)
}
