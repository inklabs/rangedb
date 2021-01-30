package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/inklabs/rangedb/pkg/aggregategenerator"
	"github.com/inklabs/rangedb/pkg/structparser"
)

func main() {
	pkg := flag.String("package", os.Getenv("GOPACKAGE"), "package")
	aggregateName := flag.String("name", "", "aggregate name")
	commandsPath := flag.String("commands", "", "filename containing commands")
	outFilePath := flag.String("out", "", "output file name")
	flag.Parse()

	if *outFilePath == "" {
		*outFilePath = fmt.Sprintf("%s_aggregate_gen.go", *aggregateName)
	}

	commandsFile, err := os.Open(*commandsPath)
	if err != nil {
		log.Fatalf("unable to open (%s): %v", *commandsPath, err)
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

	err = aggregategenerator.Write(outFile, commands, *pkg, *aggregateName)
	if err != nil {
		log.Fatalf("unable to write to aggregates file: %v", err)
	}
}
