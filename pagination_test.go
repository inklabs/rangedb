package rangedb_test

import (
	"fmt"
	"github.com/inklabs/rangedb/pkg/paging"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPagination(t *testing.T) {
	// Given
	paginationTests := []struct {
		itemsPerPage         string
		page                 string
		expectedItemsPerPage int
		expectedPage         int
	}{
		{"", "", paging.DefaultItemsPerPage, paging.DefaultPage},
		{"x", "z", paging.DefaultItemsPerPage, paging.DefaultPage},
		{"-1", "-2", paging.DefaultItemsPerPage, paging.DefaultPage},
		{"0", "0", paging.DefaultItemsPerPage, paging.DefaultPage},
		{"1", "1", 1, 1},
		{"10", "1", 10, 1},
		{"500", "20", 500, 20},
		{"99999", "20", paging.MaxItemsPerPage, 20},
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
