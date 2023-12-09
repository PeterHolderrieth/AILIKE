package godb

import (
	"fmt"
	"os"
	"time"
)

type BenchMetaData struct {
	catalog   string // name of the catalog file
	dbDir     string // dir containing the catalog and associated data files
	bpSize    int
	outputDir string // name of the dir to output results
	save      bool   // whether to save the query results and timing results
}

func newBenchMetaData(catalog string, dbDir string, bpSize int, outputDir string, save bool) BenchMetaData {
	return BenchMetaData{catalog: catalog, dbDir: dbDir, bpSize: bpSize, outputDir: outputDir, save: save}
}

// BenchmarkingInfra runs a query and:
//   - saves the results in a file titled by queryName
//   - saves the time taken to run the query
//   - returns the time taken to run the query
func BenchmarkingInfra(queryName string, query string, config BenchMetaData) (int64, error) {
	bp := NewBufferPool(config.bpSize)
	c, err := NewCatalogFromFile(config.catalog, bp, config.dbDir)
	if err != nil {
		return 0, err
	}
	qType, plan, err := Parse(c, query)
	if err != nil {
		return 0, err
	}
	if plan == nil {
		return 0, GoDBError{ParseError, "Plan was nil"}
	}
	if qType != IteratorType {
		return 0, GoDBError{ParseError, "Plan is not of iterator type"}
	}
	desc := plan.Descriptor()
	if desc == nil {
		return 0, GoDBError{ParseError, "Descriptor was nil"}
	}
	tid := NewTID()
	iter, err := plan.Iterator(tid)
	if err != nil {
		return 0, err
	}

	// Run once to collect timing information
	start := time.Now()
	for {
		tup, err := iter()

		// Comment out this check while actually
		// benchmarking, here right now for debugging help
		if err != nil {
			return 0, err
		}
		if tup == nil {
			break
		}
	}
	end := time.Since(start)

	// Now save output
	timing_csv_path := config.outputDir + "/" + "ALL_TIMINGS.csv"
	if queryName == "ALL_TIMINGS" {
		panic("Query name cannot be ALL_TIMINGS.")
	}
	output_csv_path := config.outputDir + "/" + queryName + ".csv"
	if config.save {
		timing_csv, err := os.OpenFile(timing_csv_path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return 0, GoDBError{OSError, err.Error()}
		}
		fmt.Fprintf(timing_csv, "%s, %v\n", queryName, end.Milliseconds())

		outfile_csv, err := os.OpenFile(output_csv_path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return 0, GoDBError{OSError, err.Error()}
		}
		// fmt.Fprintf(outfile_csv, "%s\n", plan.Descriptor().HeaderString(false))
		iter, err = plan.Iterator(tid)
		if err != nil {
			return 0, err
		}
		for {
			tup, err := iter()
			if err != nil {
				return 0, err
			}
			if tup == nil {
				break
			}
			fmt.Fprintf(outfile_csv, "%s\n", tup.Fields[1].(EmbeddedStringField).Value)
		}
	}

	return end.Milliseconds(), nil
}
