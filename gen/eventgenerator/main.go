package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/inklabs/rangedb/pkg/eventparser"
)

func main() {
	pkg := flag.String("package", os.Getenv("GOPACKAGE"), "package")
	id := flag.String("id", "", "id")
	aggregateType := flag.String("aggregateType", "", "stream identifier")
	inFilePath := flag.String("inFile", os.Getenv("GOFILE"), "input filename containing structs")
	outFilePath := flag.String("outFile", "", "output filename containing generated struct methods")
	flag.Parse()

	file, err := os.Open(*inFilePath)
	if err != nil {
		log.Fatalf("unable to open (%s): %v", *inFilePath, err)
	}

	if *outFilePath == "" {
		fileName := strings.TrimSuffix(*inFilePath, path.Ext(*inFilePath))
		*outFilePath = fmt.Sprintf("%s_gen.go", fileName)
	}

	events, err := eventparser.GetEvents(file)
	if err != nil {
		log.Fatalf("unable to extract events: %v", err)
	}

	_ = file.Close()

	outFile, err := os.Create(*outFilePath)
	if err != nil {
		log.Fatalf("unable to create events file: %v", err)
	}

	err = eventparser.WriteEvents(outFile, events, *pkg, *id, *aggregateType)
	if err != nil {
		log.Fatalf("unable to write to events file: %v", err)
	}

	_ = outFile.Close()
}
