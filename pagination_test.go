package rangedb_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
)

func TestNewPagination(t *testing.T) {
	// Given
	paginationTests := []struct {
		itemsPerPage         string
		page                 string
		expectedItemsPerPage int
		expectedPage         int
	}{
		{"", "", rangedb.DefaultItemsPerPage, rangedb.DefaultPage},
		{"x", "z", rangedb.DefaultItemsPerPage, rangedb.DefaultPage},
		{"-1", "-2", rangedb.DefaultItemsPerPage, rangedb.DefaultPage},
		{"0", "0", rangedb.DefaultItemsPerPage, rangedb.DefaultPage},
		{"1", "1", 1, 1},
		{"10", "1", 10, 1},
		{"500", "20", 500, 20},
		{"99999", "20", rangedb.MaxItemsPerPage, 20},
	}

	for _, tt := range paginationTests {
		name := fmt.Sprintf("items:%s,page:%s->items:%d,page:%d",
			tt.itemsPerPage, tt.page, tt.expectedItemsPerPage, tt.expectedPage)
		t.Run(name, func(t *testing.T) {
			// When
			pagination := rangedb.NewPaginationFromString(tt.itemsPerPage, tt.page)

			// Then
			assert.Equal(t, tt.expectedItemsPerPage, pagination.ItemsPerPage)
			assert.Equal(t, tt.expectedPage, pagination.Page)
		})
	}
}
