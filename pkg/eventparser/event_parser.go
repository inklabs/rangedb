package eventparser

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"strings"

	"github.com/fatih/structtag"
)

type PersonalData struct {
	SubjectID        string
	Fields           []string
	SerializedFields map[string]string
}

type ParsedEvent struct {
	Name         string
	PersonalData *PersonalData
}

func GetEvents(reader io.Reader) ([]ParsedEvent, error) {
	node, err := parser.ParseFile(token.NewFileSet(), "", reader, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed parsing: %v", err)
	}

	structVisitor := &structVisitor{}
	ast.Walk(structVisitor, node)

	if structVisitor.err != nil {
		return nil, structVisitor.err
	}

	return structVisitor.ParsedEvents, nil
}

type structVisitor struct {
	ParsedEvents []ParsedEvent
	err          error
}

func (v *structVisitor) Visit(node ast.Node) (w ast.Visitor) {
	switch n := node.(type) {
	case *ast.TypeSpec:
		if value, ok := n.Type.(*ast.StructType); ok {
			var subjectID string
			var personalDataFields []string
			var serializedFields map[string]string
			for _, field := range value.Fields.List {
				// log.Printf(" %v - %v", field.Names, field.Tag)
				if field.Tag == nil || len(field.Names) == 0 {
					continue
				}

				fieldName := field.Names[0].String()
				tagString := strings.Replace(field.Tag.Value, "`", "", 2)
				tags, err := structtag.Parse(tagString)
				if err != nil {
					continue
				}

				encryptTag, err := tags.Get("encrypt")
				if err != nil {
					continue
				}

				switch encryptTag.Value() {
				case "subject-id":
					subjectID = fieldName

				case "personal-data":
					serializedTag, err := tags.Get("serialized")
					if err != nil {
						personalDataFields = append(personalDataFields, fieldName)
					} else {
						if serializedFields == nil {
							serializedFields = make(map[string]string)
						}
						serializedFields[serializedTag.Name] = fieldName
					}
				}
			}

			for _, field := range value.Fields.List {
				if len(field.Names) == 0 {
					continue
				}

				fieldName := field.Names[0].String()
				if _, ok := serializedFields[fieldName]; ok {
					if fmt.Sprintf("%s", field.Type) != "string" {
						v.err = fmt.Errorf("no string target for serialized personal data")
					}
				}
			}

			var parsedEvent ParsedEvent
			if subjectID != "" {
				parsedEvent = ParsedEvent{
					Name: n.Name.String(),
					PersonalData: &PersonalData{
						SubjectID:        subjectID,
						Fields:           personalDataFields,
						SerializedFields: serializedFields,
					},
				}
			} else {
				parsedEvent = ParsedEvent{
					Name:         n.Name.String(),
					PersonalData: nil,
				}
			}

			v.ParsedEvents = append(v.ParsedEvents, parsedEvent)
		}
	}

	return v
}
