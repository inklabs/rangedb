package memorytemplate

import (
	"compress/zlib"
	"encoding/base64"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager"
)

type memoryTemplateManager struct {
	templates map[string]*template.Template
}

func New(base64Templates map[string]string) (*memoryTemplateManager, error) {
	templates, err := loadTemplates(base64Templates)
	if err != nil {
		return nil, err
	}
	return &memoryTemplateManager{
		templates: templates,
	}, nil
}

func (r *memoryTemplateManager) RenderTemplate(w io.Writer, templateName string, data interface{}) error {
	tmpl, ok := r.templates[templateName]
	if !ok {
		return templatemanager.TemplateNotFound
	}

	err := tmpl.Execute(w, data)
	if err != nil {
		return fmt.Errorf("template execution failed: %v", err)
	}

	return nil
}

func loadTemplates(base64Templates map[string]string) (map[string]*template.Template, error) {
	var layoutFiles, includeFiles []string

	for templateName := range base64Templates {
		if strings.HasPrefix(templateName, "layout/") {
			layoutFiles = append(layoutFiles, templateName)
		} else {
			includeFiles = append(includeFiles, templateName)
		}
	}

	templates := make(map[string]*template.Template)

	for _, file := range includeFiles {
		fileName := filepath.Base(file)
		files := append(layoutFiles, file)
		templates[fileName] = template.New(fileName).Funcs(templatemanager.FuncMap)
		err := parseFiles(base64Templates, templates[fileName], files)
		if err != nil {
			return nil, err
		}
	}

	return templates, nil
}

func parseFiles(base64Templates map[string]string, t *template.Template, files []string) error {
	for _, file := range files {
		base64Decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(base64Templates[file]))
		zlibReader, err := zlib.NewReader(base64Decoder)
		if err != nil {
			return fmt.Errorf("unable to read template content: %v", err)
		}

		text, err := ioutil.ReadAll(zlibReader)
		if err != nil {
			return fmt.Errorf("unable to read zlib compressed template content: %v", err)
		}

		_, err = t.Parse(string(text))
		if err != nil {
			return fmt.Errorf("unable to parse template content: %v", err)
		}
	}
	return nil
}
