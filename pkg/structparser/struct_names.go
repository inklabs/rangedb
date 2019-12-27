package structparser

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
)

func GetStructNames(file io.Reader) ([]string, error) {
	node, err := parser.ParseFile(token.NewFileSet(), "", file, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed parsing: %v", err)
	}

	structVisitor := &structVisitor{}
	ast.Walk(structVisitor, node)

	return structVisitor.Names, nil
}

type structVisitor struct {
	Names []string
}

func (v *structVisitor) Visit(node ast.Node) (w ast.Visitor) {
	switch n := node.(type) {
	case *ast.TypeSpec:
		if _, ok := n.Type.(*ast.StructType); ok {
			v.Names = append(v.Names, n.Name.String())
		}
	}

	return v
}
