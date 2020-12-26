package templatemanager

import (
	"errors"
	"io"
)

// TemplateManager defines a template manager for rendering templates.
type TemplateManager interface {
	RenderTemplate(w io.Writer, templateName string, data interface{}) error
}

// TemplateNotFound defines a template not found error.
var TemplateNotFound = errors.New("template not found")
