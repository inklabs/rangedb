package templatemanager

import (
	"errors"
	"io"
)

// TemplateManager defines a template manager for rendering templates.
type TemplateManager interface {
	RenderTemplate(w io.Writer, templateName string, data interface{}) error
}

// ErrTemplateNotFound defines a template not found error.
var ErrTemplateNotFound = errors.New("template not found")
