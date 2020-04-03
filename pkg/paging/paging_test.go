package paging_test

import (
	"github.com/inklabs/rangedb/pkg/paging"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewLinks_HasNextPage(t *testing.T) {
	// Given
	totalRecords := uint64(100)
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
