package paging

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	defaultItemsPerPage = 10
	defaultIndex        = 0
	maxItemsPerPage     = 1000
)

// Pagination contains page information for building pagination Links.
type Pagination struct {
	ItemsPerPage    uint64
	CurrentIndex    uint64
	PreviousIndices []uint64
}

// Links contains previous/next URL links for pagination.
type Links struct {
	Previous string
	Next     string
}

// NewPagination constructs a Pagination object.
func NewPagination(itemsPerPage, currentIndex uint64, previousIndices []uint64) Pagination {
	if itemsPerPage <= 0 {
		itemsPerPage = defaultItemsPerPage
	}

	if itemsPerPage > maxItemsPerPage {
		itemsPerPage = maxItemsPerPage
	}

	return Pagination{
		ItemsPerPage:    itemsPerPage,
		CurrentIndex:    currentIndex,
		PreviousIndices: previousIndices,
	}
}

// NewPaginationFromQuery constructs a Pagination from a URL query.
func NewPaginationFromQuery(values url.Values) Pagination {
	itemsPerPage := values.Get("itemsPerPage")
	previousIndices := values.Get("previous")
	currentIndex := values.Get("current")

	return NewPaginationFromString(itemsPerPage, currentIndex, previousIndices)
}

// NewPaginationFromString constructs a Pagination from string input.
func NewPaginationFromString(itemsPerPageInput, currentIndexInput, previousIndexInput string) Pagination {
	itemsPerPage, err := strconv.ParseUint(itemsPerPageInput, 10, 64)
	if err != nil {
		itemsPerPage = defaultItemsPerPage
	}

	var previousIndices []uint64
	for _, previousIndexString := range strings.Split(previousIndexInput, ",") {
		previousIndex, err := strconv.ParseUint(previousIndexString, 10, 64)
		if err != nil {
			previousIndex = defaultIndex
			break
		}

		previousIndices = append(previousIndices, previousIndex)
	}

	currentIndex, err := strconv.ParseUint(currentIndexInput, 10, 64)
	if err != nil {
		currentIndex = defaultIndex
	}

	return NewPagination(itemsPerPage, currentIndex, previousIndices)
}

// Links returns the previous/next links.
func (p Pagination) Links(baseURI string, nextIndex uint64) Links {
	previousURI := ""
	nextURI := ""

	if len(p.PreviousIndices) > 0 {
		u, _ := url.Parse(baseURI)

		q := url.Values{}
		q.Add("itemsPerPage", strconv.FormatUint(p.ItemsPerPage, 10))

		previousIndices := p.PreviousIndices[:len(p.PreviousIndices)-1]
		if len(previousIndices) > 0 {
			previousCSV := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(previousIndices)), ","), "[]")
			q.Add("previous", previousCSV)
		}

		current := p.PreviousIndices[len(p.PreviousIndices)-1]
		q.Add("current", strconv.FormatUint(current, 10))

		u.RawQuery = q.Encode()
		previousURI = u.String()
	}

	if nextIndex > p.CurrentIndex {
		u, _ := url.Parse(baseURI)

		q := url.Values{}
		q.Add("itemsPerPage", strconv.FormatUint(p.ItemsPerPage, 10))

		previousIndices := append(p.PreviousIndices, p.CurrentIndex)
		previousCSV := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(previousIndices)), ","), "[]")
		q.Add("previous", previousCSV)

		q.Add("current", strconv.FormatUint(nextIndex, 10))

		u.RawQuery = q.Encode()
		nextURI = u.String()
	}

	return Links{
		Previous: previousURI,
		Next:     nextURI,
	}
}
