package rangedbui_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/pkg/rangedbui"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager/provider/filesystemtemplate"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager/provider/memorytemplate"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_Index(t *testing.T) {
	// Given
	templateManager, err := memorytemplate.New(rangedbui.GetTemplates())
	require.NoError(t, err)
	ui := rangedbui.New(templateManager, nil, nil)
	request := httptest.NewRequest("GET", "/", nil)
	response := httptest.NewRecorder()

	// When
	ui.ServeHTTP(response, request)

	// Then
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
}

func Test_ListAggregateTypes(t *testing.T) {
	// Given
	store, aggregateTypeStats := storeWithTwoEvents()

	t.Run("works with memory loader", func(t *testing.T) {
		// Given
		templateManager, err := memorytemplate.New(rangedbui.GetTemplates())
		require.NoError(t, err)
		ui := rangedbui.New(templateManager, aggregateTypeStats, store)
		request := httptest.NewRequest("GET", "/aggregate-types", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing")
		assert.Contains(t, response.Body.String(), "another")
	})

	t.Run("works with filesystem loader", func(t *testing.T) {
		// Given
		templateManager := filesystemtemplate.New("./templates")
		ui := rangedbui.New(templateManager, aggregateTypeStats, store)
		request := httptest.NewRequest("GET", "/aggregate-types", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing")
		assert.Contains(t, response.Body.String(), "another")
	})
}

func Test_AggregateType(t *testing.T) {
	// Given
	templateManager, err := memorytemplate.New(rangedbui.GetTemplates())
	require.NoError(t, err)
	store, aggregateTypeStats := storeWithTwoEvents()
	ui := rangedbui.New(templateManager, aggregateTypeStats, store)

	t.Run("renders events by aggregate type", func(t *testing.T) {
		// Given
		request := httptest.NewRequest("GET", "/e/thing", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing")
		assert.Contains(t, response.Body.String(), "Aggregate Type: thing")
		assert.Contains(t, response.Body.String(), "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac")
	})

	t.Run("renders events by stream", func(t *testing.T) {
		// Given
		request := httptest.NewRequest("GET", "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing")
		assert.Contains(t, response.Body.String(), "Stream: thing!f6b6f8ed682c4b5180f625e53b3c4bac")
		assert.Contains(t, response.Body.String(), "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac")
	})
}

func Test_ServesStaticAssets(t *testing.T) {
	// Given
	templateManager, err := memorytemplate.New(rangedbui.GetTemplates())
	require.NoError(t, err)
	ui := rangedbui.New(templateManager, nil, nil)
	request := httptest.NewRequest("GET", "/static/css/foundation-6.5.3.min.css", nil)
	response := httptest.NewRecorder()

	// When
	ui.ServeHTTP(response, request)

	// Then
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "text/css; charset=utf-8", response.Header().Get("Content-Type"))
}

func storeWithTwoEvents() (rangedb.Store, *projection.AggregateTypeStats) {
	store := inmemorystore.New()
	aggregateTypeStats := projection.NewAggregateTypeStats()
	store.Subscribe(aggregateTypeStats)

	_ = store.SaveEvent(
		"thing",
		"f6b6f8ed682c4b5180f625e53b3c4bac",
		"ThingWasDone",
		"cf20808548c84706b8cd7b611872eaa4",
		rangedbtest.ThingWasDone{
			ID:     "cf20808548c84706b8cd7b611872eaa4",
			Number: 0,
		},
		nil,
	)

	_ = store.SaveEvent(
		"another",
		"d2966d9ecb344d96a919655e2980fc11",
		"AnotherWasComplete",
		"5e4a649230924041a7ccf18887ccc153",
		rangedbtest.AnotherWasComplete{
			ID: "5e4a649230924041a7ccf18887ccc153",
		},
		nil,
	)

	return store, aggregateTypeStats
}
