package filesystemtemplate

import (
	"fmt"
	"html/template"
	"io"
	"path/filepath"

	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager"
)

type filesystemTemplateManager struct {
	rootPath string
}

func New(rootPath string) *filesystemTemplateManager {
	return &filesystemTemplateManager{
		rootPath: rootPath,
	}
}

func (r *filesystemTemplateManager) RenderTemplate(w io.Writer, templateName string, data interface{}) error {
	templates, err := loadTemplates(r.rootPath)
	if err != nil {
		return err
	}

	tmpl, ok := templates[templateName]
	if !ok {
		return templatemanager.TemplateNotFound
	}

	err = tmpl.Execute(w, data)
	if err != nil {
		return fmt.Errorf("template execution failed: %v", err)
	}

	return nil
}

func loadTemplates(rootPath string) (map[string]*template.Template, error) {
	layoutFiles, err := filepath.Glob(rootPath + "/layout/*.html")
	if err != nil {
		return nil, fmt.Errorf("unable to load layout templates: %v", err)
	}

	includeFiles, _ := filepath.Glob(rootPath + "/*.html")

	templates := make(map[string]*template.Template)

	for _, file := range includeFiles {
		fileName := filepath.Base(file)
		files := append(layoutFiles, file)
		tmpl, err := template.New(fileName).Funcs(templatemanager.FuncMap).ParseFiles(files...)

		if err != nil {
			return nil, err
		}

		templates[fileName] = tmpl
	}

	return templates, nil
}
