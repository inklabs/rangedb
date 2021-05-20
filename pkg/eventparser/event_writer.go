package eventparser

import (
	"fmt"
	"io"
	"text/template"
	"time"
)

var NowFunc = time.Now

func WriteEvents(out io.Writer, events []ParsedEvent, pkg, id, aggregateType string) error {
	if len(events) < 1 {
		return fmt.Errorf("no events found")
	}

	containsPersonalData := false
	containsSerializedData := false
	for _, event := range events {
		if event.PersonalData != nil {
			if event.PersonalData.SerializedFields != nil {
				containsSerializedData = true
			}
			containsPersonalData = true
		}
	}

	return fileTemplate.Execute(out, templateData{
		Timestamp:              NowFunc(),
		Events:                 events,
		AggregateType:          aggregateType,
		ID:                     id,
		Package:                pkg,
		ContainsPersonalData:   containsPersonalData,
		ContainsSerializedData: containsSerializedData,
	})
}

type templateData struct {
	Timestamp              time.Time
	Events                 []ParsedEvent
	AggregateType          string
	ID                     string
	Package                string
	ContainsPersonalData   bool
	ContainsSerializedData bool
}

var fileTemplate = template.Must(template.New("").Parse(`// Code generated by go generate; DO NOT EDIT.
// This file was generated at
// {{ .Timestamp }}
package {{ $.Package }}

{{- if .ContainsPersonalData }}

import (
{{- if .ContainsSerializedData }}
	"strconv"
{{ end }}
	"github.com/inklabs/rangedb/pkg/crypto"
)
{{- end }}

{{- range .Events }}

func (e {{ .Name }}) AggregateID() string   { return e.{{ $.ID }} }
func (e {{ .Name }}) AggregateType() string { return "{{ $.AggregateType }}" }
func (e {{ .Name }}) EventType() string     { return "{{ .Name }}" }

{{- if .PersonalData }}
{{- $event := . }}
func (e *{{ .Name }}) Encrypt(encryptor crypto.Encryptor) error {
	var err error
{{- range $event.PersonalData.Fields }}
	e.{{ . }}, err = encryptor.Encrypt(e.{{ $event.PersonalData.SubjectID }}, e.{{ . }})
	if err != nil {
		e.RedactPersonalData("")
		return err
	}
{{ end }}
{{- range $key, $value := $event.PersonalData.SerializedFields }}
	string{{ $value }} := strconv.Itoa(e.{{ $value }})
	e.{{ $key }}, err = encryptor.Encrypt(e.{{ $event.PersonalData.SubjectID }}, string{{ $value }})
	if err != nil {
		{{- range $k, $v := $event.PersonalData.SerializedFields }}
		e.{{ $v }} = 0
		{{- end }}
		return err
	}
	e.{{ $value }} = 0
{{ end }}
	return nil
}
func (e *{{ .Name }}) Decrypt(encryptor crypto.Encryptor) error {
	var err error{{ range $event.PersonalData.Fields }}
	e.{{ . }}, err = encryptor.Decrypt(e.{{ $event.PersonalData.SubjectID }}, e.{{ . }})
	if err != nil {
		e.RedactPersonalData("")
		return err
	}
{{ end }}
{{- range $key, $value := $event.PersonalData.SerializedFields }}
	decrypted{{ $value }}, err := encryptor.Decrypt(e.{{ $event.PersonalData.SubjectID }}, e.{{ $key }})
	if err != nil {
		{{- range $k, $v := $event.PersonalData.SerializedFields }}
		e.{{ $v }} = 0
		{{- end }}
		return err
	}
	e.{{ $value }}, err = strconv.Atoi(decrypted{{ $value }})
	if err != nil {
		{{- range $k, $v := $event.PersonalData.SerializedFields }}
		e.{{ $v }} = 0
		{{- end }}
		return err
	}
	e.{{ $key }} = ""
{{ end }}
	return nil
}
func (e *{{ .Name }}) RedactPersonalData(redactTo string) error {
	{{- range $event.PersonalData.Fields }}
	e.{{ . }} = redactTo
	{{- end }}
}
{{- end }}
{{- end }}
`))
