package templatemanager

import (
	"errors"
	"io"
)

type TemplateManager interface {
	RenderTemplate(w io.Writer, templateName string, data interface{}) error
}

var TemplateNotFound = errors.New("template not found")
