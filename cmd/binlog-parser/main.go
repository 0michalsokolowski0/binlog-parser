package main

import (
	"0michalsokolowski0/binlog-parser/internal/database"
	"0michalsokolowski0/binlog-parser/internal/parser"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/sirupsen/logrus"
)

var prettyPrintJsonFlag = flag.Bool("prettyprint", false, "Pretty print json")
var includeTablesFlag = flag.String("include_tables", "", "comma-separated list of tables to include")
var includeSchemasFlag = flag.String("include_schemas", "", "comma-separated list of schemas to include")

func main() {
	flag.Usage = func() {
		printUsage()
	}

	flag.Parse()

	if flag.NArg() != 1 {
		printUsage()
		os.Exit(1)
	}

	binlogFilename := flag.Arg(0)
	dbDsn := os.Getenv("DB_DSN")

	if dbDsn == "" {
		fmt.Fprint(os.Stderr, "Please set env variable DB_DSN to a valid MySQL connection string")
		os.Exit(1)
	}

	logrus.Infof("Will binlog-parser file %s", binlogFilename)

	parseFunc := createBinlogParseFunc(dbDsn, consumerChainFromArgs())
	err := parseFunc(binlogFilename)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Got error: %s\n", err)
		os.Exit(1)
	}
}

func consumerChainFromArgs() parser.ConsumerChain {
	chain := parser.NewConsumerChain()

	chain.CollectAsJson(os.Stdout, *prettyPrintJsonFlag)
	logrus.Infof("Pretty print JSON %s", *prettyPrintJsonFlag)

	if *includeTablesFlag != "" {
		includeTables := commaSeparatedListToArray(*includeTablesFlag)

		chain.IncludeTables(includeTables...)
		logrus.Infof("Including tables %v", includeTables)
	}

	if *includeSchemasFlag != "" {
		includeSchemas := commaSeparatedListToArray(*includeSchemasFlag)

		chain.IncludeSchemas(includeSchemas...)
		logrus.Infof("Including schemas %v", includeSchemas)
	}

	return chain
}

func printUsage() {
	binName := path.Base(os.Args[0])

	usage := "Parse a binlog file, dump JSON to stdout. Includes options to filter by schema and table.\n" +
		"Reads from information_schema database to find out the field names for a row event.\n\n" +
		"Usage:\t%s [options ...] binlog\n\n" +
		"Options are:\n\n"

	fmt.Fprintf(os.Stderr, usage, binName)

	flag.PrintDefaults()

	envVars := "\nRequired environment variables:\n\n" +
		"DB_DSN\t Database connection string, needs read access to information_schema\n"

	fmt.Fprint(os.Stderr, envVars)
}

func commaSeparatedListToArray(str string) []string {
	var arr []string

	for _, item := range strings.Split(str, ",") {
		item = strings.TrimSpace(item)

		if item != "" {
			arr = append(arr, item)
		}
	}

	return arr
}

type binlogParseFunc func(string) error

func createBinlogParseFunc(dbDsn string, consumerChain parser.ConsumerChain) binlogParseFunc {
	return func(binlogFilename string) error {
		return parseBinlogFile(binlogFilename, dbDsn, consumerChain)
	}
}

func parseBinlogFile(binlogFilename, dbDsn string, consumerChain parser.ConsumerChain) error {
	logrus.Infof("Parsing binlog file %s", binlogFilename)

	db, err := database.GetDatabaseInstance(dbDsn)

	if err != nil {
		return err
	}

	defer db.Close()

	tableMap := database.NewTableMap(db)

	logrus.Info("About to binlog-parser file ...")

	return parser.ParseBinlog(binlogFilename, tableMap, consumerChain)
}
