package main

import (
	"flag"
	"io"
	"log"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/inklabs/rangedb/pkg/structparser"
)

func main() {
	packageName := flag.String("package", os.Getenv("GOPACKAGE"), "package")
	fileNamesCSV := flag.String("files", "", "comma separated filenames containing events")
	outFilePath := flag.String("out", "bind_events_gen.go", "output file name")
	flag.Parse()

	fileNames := strings.Split(*fileNamesCSV, ",")

	var eventNames []string
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("unable to open (%s): %v", fileName, err)
		}

		structNames, err := structparser.GetStructNames(file)
		if err != nil {
			log.Fatalf("unable to extract events: %v", err)
		}

		eventNames = append(eventNames, structNames...)
	}

	writeEventBinder(eventNames, *packageName, *outFilePath)
}

func writeEventBinder(eventNames []string, packageName, outputFilePath string) {
	file, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("unable to create events file: %v", err)
	}
	defer closeOrFail(file)

	err = fileTemplate.Execute(file, templateData{
		Timestamp:   time.Now(),
		PackageName: packageName,
		EventNames:  eventNames,
	})
	if err != nil {
		log.Fatalf("unable to write to events file: %v", err)
	}
}

func closeOrFail(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Fatalf("failed closing: %v", err)
	}
}

type templateData struct {
	Timestamp   time.Time
	PackageName string
	EventNames  []string
}

var fileTemplate = template.Must(template.New("").Parse(`// Code generated by go generate; DO NOT EDIT.
// This file was generated at
// {{ .Timestamp }}
package {{ .PackageName }}

import "github.com/inklabs/rangedb"

func BindEvents(binder rangedb.EventBinder) {
	binder.Bind({{ range .EventNames }}
		&{{ . }}{},{{ end }}
	)
}
`))
