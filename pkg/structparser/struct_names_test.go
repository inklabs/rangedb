package structparser_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/structparser"
)

func TestGetStructNames(t *testing.T) {
	t.Run("returns empty list", func(t *testing.T) {
		// Given
		code := `package test`
		content := strings.NewReader(code)

		// When
		structNames, err := structparser.GetStructNames(content)

		// Then
		require.NoError(t, err)
		assert.Equal(t, 0, len(structNames))
	})

	t.Run("returns one struct name with other keywords present", func(t *testing.T) {
		// Given
		code := `package test
type Struct1 struct {}
type Interface1 interface {}
func Func1() {}`
		content := strings.NewReader(code)

		// When
		structNames, err := structparser.GetStructNames(content)

		// Then
		require.NoError(t, err)
		assert.Equal(t, 1, len(structNames))
		assert.Equal(t, []string{"Struct1"}, structNames)
	})

	t.Run("returns two struct names", func(t *testing.T) {
		// Given
		code := `package test
type Struct1 struct {}
type Struct2 struct{}`
		content := strings.NewReader(code)

		// When
		structNames, err := structparser.GetStructNames(content)

		// Then
		require.NoError(t, err)
		assert.Equal(t, []string{"Struct1", "Struct2"}, structNames)
	})

	t.Run("fails with empty reader", func(t *testing.T) {
		// Given
		emptyReader := strings.NewReader("")

		// When
		structNames, err := structparser.GetStructNames(emptyReader)

		// Then
		assert.Nil(t, structNames)
		assert.EqualError(t, err, "failed parsing: 1:1: expected 'package', found 'EOF'")
	})
}
