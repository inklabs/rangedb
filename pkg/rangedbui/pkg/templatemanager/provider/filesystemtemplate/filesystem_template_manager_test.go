package filesystemtemplate_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager/provider/filesystemtemplate"
)

func TestFilesystemTemplateManager_RenderTemplate(t *testing.T) {
	t.Run("fails when template not found", func(t *testing.T) {
		// Given
		templateManager := filesystemtemplate.New("./testdata/valid_templates")
		var buf bytes.Buffer

		// When
		err := templateManager.RenderTemplate(&buf, "invalid-name", nil)

		// Then
		assert.Equal(t, templatemanager.ErrTemplateNotFound, err)
	})

	t.Run("fails when root path impacts the glob pattern", func(t *testing.T) {
		// Given
		templateManager := filesystemtemplate.New("[-]")
		var buf bytes.Buffer

		// When
		err := templateManager.RenderTemplate(&buf, "invalid-name", nil)

		// Then
		assert.EqualError(t, err, "unable to load layout templates: syntax error in pattern")
	})

	t.Run("fails when template is invalid", func(t *testing.T) {
		// Given
		templateManager := filesystemtemplate.New("./testdata/invalid_templates")
		var buf bytes.Buffer

		// When
		err := templateManager.RenderTemplate(&buf, "invalid-template.html", nil)

		// Then
		assert.EqualError(t, err, "template: invalid-template.html:4: unclosed action started at invalid-template.html:3")
	})

	t.Run("fails when unable to write", func(t *testing.T) {
		// Given
		templateManager := filesystemtemplate.New("./testdata/valid_templates")
		var failingWriter failWriter

		// When
		err := templateManager.RenderTemplate(&failingWriter, "valid-template.html", nil)

		// Then
		assert.EqualError(t, err, "template execution failed: short write")
	})
}

type failWriter bool

func (w *failWriter) Write(_ []byte) (int, error) {
	return 0, io.ErrShortWrite
}
