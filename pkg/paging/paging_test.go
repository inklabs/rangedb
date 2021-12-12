package paging_test

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/paging"
)

const (
	defaultItemsPerPage = 10
	defaultPage         = 1
	maxItemsPerPage     = 1000
)

func TestPaginationScenarios(t *testing.T) {
	// Given
	paginationTests := []struct {
		itemsPerPage         string
		page                 string
		expectedItemsPerPage int
		expectedPage         int
	}{
		{"", "", defaultItemsPerPage, defaultPage},
		{"x", "z", defaultItemsPerPage, defaultPage},
		{"-1", "-2", defaultItemsPerPage, defaultPage},
		{"0", "0", defaultItemsPerPage, defaultPage},
		{"1", "1", 1, 1},
		{"10", "1", 10, 1},
		{"500", "20", 500, 20},
		{"99999", "20", maxItemsPerPage, 20},
	}

	for _, tt := range paginationTests {
		name := fmt.Sprintf("items:%s,page:%s->items:%d,page:%d",
			tt.itemsPerPage, tt.page, tt.expectedItemsPerPage, tt.expectedPage)
		t.Run(name, func(t *testing.T) {
			// When
			pagination := paging.NewPaginationFromString(tt.itemsPerPage, tt.page)

			// Then
			assert.Equal(t, tt.expectedItemsPerPage, pagination.ItemsPerPage)
			assert.Equal(t, tt.expectedPage, pagination.Page)
		})
	}
}

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
	links := pagination.Links("/a/thing", totalRecords)

	// Then
	assert.Equal(t, "/a/thing?itemsPerPage=10&page=1", links.Previous)
	assert.Equal(t, "/a/thing?itemsPerPage=10&page=3", links.Next)
}

func TestNewLinks_HasPreviousAndNoNextPage(t *testing.T) {
	// Given
	totalRecords := uint64(20)
	pagination := paging.NewPagination(10, 2)

	// When
	links := pagination.Links("/a/thing", totalRecords)

	// Then
	assert.Equal(t, "/a/thing?itemsPerPage=10&page=1", links.Previous)
	assert.Equal(t, "", links.Next)
}

func TestNewPagination_ReturnsDefaultValueForItemsPerPage(t *testing.T) {
	// When
	pagination := paging.NewPagination(0, 0)

	// Then
	assert.Equal(t, defaultItemsPerPage, pagination.ItemsPerPage)
	assert.Equal(t, defaultPage, pagination.Page)
}

func TestNewPagination_ReturnsMaxValueForItemsPerPage(t *testing.T) {
	// When
	pagination := paging.NewPagination(maxItemsPerPage+1, 1)

	// Then
	assert.Equal(t, maxItemsPerPage, pagination.ItemsPerPage)
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
	assert.Equal(t, defaultItemsPerPage, pagination.ItemsPerPage)
	assert.Equal(t, defaultPage, pagination.Page)
}
