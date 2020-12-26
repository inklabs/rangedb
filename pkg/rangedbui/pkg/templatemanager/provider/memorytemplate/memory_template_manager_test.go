package memorytemplate_test

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/rangedbui"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager/provider/memorytemplate"
)

func TestMemoryTemplateManager_RenderTemplate(t *testing.T) {
	t.Run("fails when template not found", func(t *testing.T) {
		// Given
		templateManager, err := memorytemplate.New(rangedbui.GetTemplates())
		require.NoError(t, err)
		var buf bytes.Buffer

		// When
		err = templateManager.RenderTemplate(&buf, "invalid-name", nil)

		// Then
		assert.Equal(t, templatemanager.ErrTemplateNotFound, err)
	})

	t.Run("fails when unable to parse non base64 encoded template", func(t *testing.T) {
		// Given
		base64Templates := map[string]string{
			"one": "xyz",
		}

		// When
		_, err := memorytemplate.New(base64Templates)

		// Then
		assert.EqualError(t, err, "unable to read template content: unexpected EOF")
	})

	t.Run("fails when unable to parse non zlib compressed template", func(t *testing.T) {
		// Given
		var buf bytes.Buffer
		zlibWriter := zlib.NewWriter(&buf)
		_, err := zlibWriter.Write([]byte("xyz"))
		require.NoError(t, err)
		base64Templates := map[string]string{
			"one": base64.StdEncoding.EncodeToString(buf.Bytes()),
		}

		// When
		_, err = memorytemplate.New(base64Templates)

		// Then
		assert.EqualError(t, err, "unable to read zlib compressed template content: unexpected EOF")
	})

	t.Run("fails when unable to parse template", func(t *testing.T) {
		// Given
		var buf bytes.Buffer
		zlibWriter := zlib.NewWriter(&buf)
		invalidTemplate := "{{"
		_, err := zlibWriter.Write([]byte(invalidTemplate))
		require.NoError(t, zlibWriter.Close())
		require.NoError(t, err)
		base64Templates := map[string]string{
			"one": base64.StdEncoding.EncodeToString(buf.Bytes()),
		}

		// When
		_, err = memorytemplate.New(base64Templates)

		// Then
		assert.EqualError(t, err, "unable to parse template content: template: one:1: unexpected unclosed action in command")
	})

	t.Run("fails when unable to write", func(t *testing.T) {
		// Given
		var buf bytes.Buffer
		zlibWriter := zlib.NewWriter(&buf)
		invalidTemplate := "test"
		_, err := zlibWriter.Write([]byte(invalidTemplate))
		require.NoError(t, zlibWriter.Close())
		require.NoError(t, err)
		base64Templates := map[string]string{
			"one": base64.StdEncoding.EncodeToString(buf.Bytes()),
		}
		templateManager, err := memorytemplate.New(base64Templates)
		require.NoError(t, err)
		var failingWriter failWriter

		// When
		err = templateManager.RenderTemplate(&failingWriter, "one", nil)

		// Then
		assert.EqualError(t, err, "template execution failed: short write")
	})
}

type failWriter bool

func (w *failWriter) Write(_ []byte) (int, error) {
	return 0, io.ErrShortWrite
}
