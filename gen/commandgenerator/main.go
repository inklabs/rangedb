package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/inklabs/rangedb/pkg/commandgenerator"
	"github.com/inklabs/rangedb/pkg/structparser"
)

func main() {
	pkg := flag.String("package", os.Getenv("GOPACKAGE"), "package")
	id := flag.String("id", "", "id")
	aggregateType := flag.String("aggregateType", "", "aggregate name")
	inFilePath := flag.String("inFile", os.Getenv("GOFILE"), "input filename containing commands")
	outFilePath := flag.String("outFile", "", "output file name")
	flag.Parse()

	if *outFilePath == "" {
		if *outFilePath == "" {
			fileName := strings.TrimSuffix(*inFilePath, path.Ext(*inFilePath))
			*outFilePath = fmt.Sprintf("%s_gen.go", fileName)
		}
	}

	commandsFile, err := os.Open(*inFilePath)
	if err != nil {
		log.Fatalf("unable to open (%s): %v", *inFilePath, err)
	}
	defer func() {
		_ = commandsFile.Close()
	}()

	commands, err := structparser.GetStructNames(commandsFile)
	if err != nil {
		log.Fatalf("unable to extract commands: %v", err)
	}

	outFile, err := os.Create(*outFilePath)
	if err != nil {
		log.Fatalf("unable to create aggreate file: %v", err)
	}
	defer func() {
		_ = outFile.Close()
	}()

	err = commandgenerator.Write(outFile, commands, *pkg, *id, *aggregateType)
	if err != nil {
		log.Fatalf("unable to write to aggregates file: %v", err)
	}
}
