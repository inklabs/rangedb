package aggregategenerator

import (
	"fmt"
	"io"
	"text/template"
	"time"
)

var NowFunc = time.Now

func Write(out io.Writer, commands []string, pkg, aggregateName string) error {
	if len(commands) < 1 {
		return fmt.Errorf("no commands found")
	}

	return fileTemplate.Execute(out, templateData{
		Timestamp:     NowFunc(),
		Commands:      commands,
		AggregateName: aggregateName,
		Package:       pkg,
	})
}

type templateData struct {
	Timestamp     time.Time
	Commands      []string
	AggregateName string
	Package       string
}

var fileTemplate = template.Must(template.New("").Parse(`// Code generated by go generate; DO NOT EDIT.
// This file was generated at
// {{ .Timestamp }}
package {{ $.Package }}

import (
	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

func (a *{{ .AggregateName }}) Load(recordIterator rangedb.RecordIterator) {
	for recordIterator.Next() {
		if recordIterator.Err() == nil {
			if event, ok := recordIterator.Record().Data.(rangedb.Event); ok {
				a.apply(event)
			}
		}
	}
}

func (a *{{ .AggregateName }}) Handle(command cqrs.Command) []rangedb.Event {
	switch c := command.(type) {
{{- range .Commands }}
	case {{ . }}:
		a.{{ . }}(c)
{{ end }}
	}

	defer a.resetPendingEvents()
	return a.pendingEvents
}

func (a *{{ .AggregateName }}) resetPendingEvents() {
	a.pendingEvents = nil
}

func (a *{{ .AggregateName }}) CommandTypes() []string {
	return []string{
{{- range .Commands }}
		{{ . }}{}.CommandType(),
{{- end }}
	}
}

func (a *{{ .AggregateName }}) raise(events ...rangedb.Event) {
	a.pendingEvents = append(a.pendingEvents, events...)
}
`))
