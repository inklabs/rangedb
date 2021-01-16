package eventparser_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/eventparser"
)

func TestEventParser(t *testing.T) {
	t.Run("returns no events", func(t *testing.T) {
		// Given
		code := `package test`
		content := strings.NewReader(code)

		// When
		events, err := eventparser.GetEvents(content)

		// Then
		require.NoError(t, err)
		assert.Equal(t, 0, len(events))
	})

	t.Run("returns one event with no personal data, with other keywords present", func(t *testing.T) {
		// Given
		code := `package test
type Struct1 struct {}
type Interface1 interface {}
func Func1() {}`
		content := strings.NewReader(code)

		// When
		events, err := eventparser.GetEvents(content)

		// Then
		require.NoError(t, err)
		require.Equal(t, 1, len(events))
		expected := eventparser.ParsedEvent{
			Name:         "Struct1",
			PersonalData: nil,
		}
		assert.Equal(t, expected, events[0])
	})

	t.Run("returns two events", func(t *testing.T) {
		// Given
		code := `package test
type Struct1 struct {}
type Struct2 struct{}`
		content := strings.NewReader(code)

		// When
		events, err := eventparser.GetEvents(content)

		// Then
		require.NoError(t, err)
		expected := []eventparser.ParsedEvent{
			{
				Name:         "Struct1",
				PersonalData: nil,
			},
			{
				Name:         "Struct2",
				PersonalData: nil,
			},
		}
		assert.Equal(t, expected, events)
	})

	t.Run("fails with empty reader", func(t *testing.T) {
		// Given
		emptyReader := strings.NewReader("")

		// When
		events, err := eventparser.GetEvents(emptyReader)

		// Then
		assert.Nil(t, events)
		assert.EqualError(t, err, "failed parsing: 1:1: expected 'package', found 'EOF'")
	})

	t.Run("returns one event with personal data, including json tag", func(t *testing.T) {
		// Given
		code := `package test
type Struct1 struct {
	ID     string ` + "`encrypt:\"subject-id\"`" + `
	Name   string ` + "`json:\"name\" encrypt:\"personal-data\"`" + `
	Email  string ` + "`encrypt:\"personal-data\"`" + `
}`
		content := strings.NewReader(code)

		// When
		events, err := eventparser.GetEvents(content)

		// Then
		require.NoError(t, err)
		require.Equal(t, 1, len(events))
		expected := eventparser.ParsedEvent{
			Name: "Struct1",
			PersonalData: &eventparser.PersonalData{
				SubjectID:        "ID",
				Fields:           []string{"Name", "Email"},
				SerializedFields: nil,
			},
		}
		assert.Equal(t, expected, events[0])
	})

	t.Run("returns one event with serialized personal data", func(t *testing.T) {
		// Given
		code := `package test
type Struct1 struct {
	ID               string ` + "`encrypt:\"subject-id\"`" + `
	Number           int ` + "`encrypt:\"personal-data\" serialized:\"NumberEncrypted\"`" + `
	NumberEncrypted  string
}`
		content := strings.NewReader(code)

		// When
		events, err := eventparser.GetEvents(content)

		// Then
		require.NoError(t, err)
		require.Equal(t, 1, len(events))
		expected := eventparser.ParsedEvent{
			Name: "Struct1",
			PersonalData: &eventparser.PersonalData{
				SubjectID: "ID",
				Fields:    []string(nil),
				SerializedFields: map[string]string{
					"NumberEncrypted": "Number",
				},
			},
		}
		assert.Equal(t, expected, events[0])
	})

	t.Run("returns one event with serialized personal data, and no place to store it", func(t *testing.T) {
		// Given
		code := `package test
type Struct1 struct {
	ID               string ` + "`encrypt:\"subject-id\"`" + `
	Number           int ` + "`encrypt:\"personal-data\" serialized:\"NumberEncrypted\"`" + `
	NumberEncrypted  int
}`
		content := strings.NewReader(code)

		// When
		events, err := eventparser.GetEvents(content)

		// Then
		require.EqualError(t, err, "no string target for serialized personal data")
		assert.Empty(t, events)
	})

	t.Run("returns one event with personal data, including embedded struct", func(t *testing.T) {
		// Given
		code := `package test
type Struct1 struct {
	ID       string ` + "`encrypt:\"subject-id\"`" + `
	Name     string ` + "`json:\"name\" encrypt:\"personal-data\"`" + `
	Email    string ` + "`encrypt:\"personal-data\"`" + `
	Title    string ` + "`jibberish`" + `
	Surname  string ` + "`json:\"surname\"`" + `
	EmbeddedStruct
}
type EmbeddedStruct struct {}`
		content := strings.NewReader(code)

		// When
		events, err := eventparser.GetEvents(content)

		// Then
		require.NoError(t, err)
		require.Equal(t, 2, len(events))
		expected := eventparser.ParsedEvent{
			Name: "Struct1",
			PersonalData: &eventparser.PersonalData{
				SubjectID:        "ID",
				Fields:           []string{"Name", "Email"},
				SerializedFields: nil,
			},
		}
		assert.Equal(t, expected, events[0])
	})
}
