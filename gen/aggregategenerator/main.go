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
	eventsPath := flag.String("events", "", "filename containing events")
	outFilePath := flag.String("out", "", "output file name")
	flag.Parse()

	if *outFilePath == "" {
		*outFilePath = fmt.Sprintf("%s_aggregate_gen.go", *aggregateName)
	}

	onlyAggregateNameProvided := *aggregateName != "" && *commandsPath == "" && *eventsPath == ""
	if onlyAggregateNameProvided {
		*commandsPath = fmt.Sprintf("%s_commands.go", *aggregateName)
		*eventsPath = fmt.Sprintf("%s_events.go", *aggregateName)
	}

	commands := structNamesFromPath(*commandsPath)
	events := structNamesFromPath(*eventsPath)

	outFile, err := os.Create(*outFilePath)
	if err != nil {
		log.Fatalf("unable to create aggreate file: %v", err)
	}
	defer func() {
		_ = outFile.Close()
	}()

	err = aggregategenerator.Write(outFile, *pkg, *aggregateName, commands, events)
	if err != nil {
		log.Fatalf("unable to write to aggregates file: %v", err)
	}
}

func structNamesFromPath(path string) []string {
	var structNames []string
	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			log.Fatalf("unable to open (%s): %v", path, err)
		}
		defer func() {
			_ = file.Close()
		}()

		structNames, err = structparser.GetStructNames(file)
		if err != nil {
			log.Fatalf("unable to extract struct names: %v", err)
		}
	}

	return structNames
}
