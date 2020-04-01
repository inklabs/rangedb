package rangedbui

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager/provider/filesystemtemplate"
)

func Test_renderWithValues(t *testing.T) {
	t.Run("fails from missing template", func(t *testing.T) {
		// Given
		ui := New(filesystemtemplate.New("./templates"), nil, nil)
		response := httptest.NewRecorder()

		// When
		ui.renderWithValues(response, "missing-template.html", nil)

		// Then
		assert.Equal(t, http.StatusNotFound, response.Code)
		assert.Equal(t, "text/plain; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Equal(t, "404 Not Found\n", response.Body.String())
	})

	t.Run("fails from invalid template", func(t *testing.T) {
		// Given
		path := "./pkg/templatemanager/provider/filesystemtemplate/testdata/invalid_templates"
		ui := New(filesystemtemplate.New(path), nil, nil)
		response := httptest.NewRecorder()

		// When
		ui.renderWithValues(response, "invalid-template.html", nil)

		// Then
		assert.Equal(t, http.StatusInternalServerError, response.Code)
		assert.Equal(t, "text/plain; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Equal(t, "500 Internal Server Error\n", response.Body.String())
	})
}
