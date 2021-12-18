package paging_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/paging"
)

const (
	defaultItemsPerPage = uint64(10)
	defaultIndex        = uint64(0)
	maxItemsPerPage     = uint64(1000)
)

func TestPagination_Links(t *testing.T) {
	t.Run("first page with a next page", func(t *testing.T) {
		// Given
		pagination := paging.NewPagination(1, 1, nil)

		// When
		links := pagination.Links("/foo/bar", 2)

		// Then
		assert.Equal(t, "", links.Previous)
		assert.Equal(t, "/foo/bar?current=2&itemsPerPage=1&previous=1", links.Next)
	})

	t.Run("second page with previous and next page", func(t *testing.T) {
		// Given
		pagination := paging.NewPagination(1, 2, []uint64{1})

		// When
		links := pagination.Links("/foo/bar", 3)

		// Then
		assert.Equal(t, "/foo/bar?current=1&itemsPerPage=1", links.Previous)
		assert.Equal(t, "/foo/bar?current=3&itemsPerPage=1&previous=1%2C2", links.Next)
	})

	t.Run("third page with previous and next page", func(t *testing.T) {
		// Given
		pagination := paging.NewPagination(1, 3, []uint64{1, 2})

		// When
		links := pagination.Links("/foo/bar", 4)

		// Then
		assert.Equal(t, "/foo/bar?current=2&itemsPerPage=1&previous=1", links.Previous)
		assert.Equal(t, "/foo/bar?current=4&itemsPerPage=1&previous=1%2C2%2C3", links.Next)
	})

	t.Run("fourth page with previous", func(t *testing.T) {
		// Given
		pagination := paging.NewPagination(1, 4, []uint64{1, 2, 3})

		// When
		links := pagination.Links("/foo/bar", 4)

		// Then
		assert.Equal(t, "/foo/bar?current=3&itemsPerPage=1&previous=1%2C2", links.Previous)
		assert.Equal(t, "", links.Next)
	})
}

func TestNewPagination(t *testing.T) {
	t.Run("returns default value for itemsPerPage", func(t *testing.T) {
		// Given

		// When
		pagination := paging.NewPagination(0, 0, nil)

		// Then
		assert.Equal(t, defaultItemsPerPage, pagination.ItemsPerPage)
		assert.Equal(t, []uint64(nil), pagination.PreviousIndices)
		assert.Equal(t, defaultIndex, pagination.CurrentIndex)
	})

	t.Run("returns max value for itemsPerPage", func(t *testing.T) {
		// Given

		// When
		pagination := paging.NewPagination(maxItemsPerPage+1, 1, []uint64{1})

		// Then
		assert.Equal(t, maxItemsPerPage, pagination.ItemsPerPage)
	})
}

func TestNewPaginationFromQuery(t *testing.T) {
	// Given
	values := url.Values{}
	values.Set("itemsPerPage", "1")
	values.Set("previous", "1,2,3")
	values.Set("current", "4")

	// When
	pagination := paging.NewPaginationFromQuery(values)

	// Then
	assert.Equal(t, uint64(1), pagination.ItemsPerPage)
	assert.Equal(t, []uint64{1, 2, 3}, pagination.PreviousIndices)
	assert.Equal(t, uint64(4), pagination.CurrentIndex)
}

func TestNewPaginationFromQuery_UsingDefaultValues(t *testing.T) {
	// Given
	values := url.Values{}

	// When
	pagination := paging.NewPaginationFromQuery(values)

	// Then
	assert.Equal(t, defaultItemsPerPage, pagination.ItemsPerPage)
	assert.Equal(t, defaultIndex, pagination.CurrentIndex)
}
