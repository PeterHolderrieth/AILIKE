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

// BenchmarkingInfra runs a query and:
//   - saves the results in a file titled by queryName
//   - saves the time taken to run the query
//   - returns the time taken to run the query
func BenchmarkingInfra(queryName string, query string, config BenchMetaData) (time.Duration, error) {
	bp := NewBufferPool(config.bpSize)
	c, err := NewCatalogFromFile(config.catalog, bp, config.dbDir)
	if err != nil {
		return time.Duration(0), err
	}
	qType, plan, err := Parse(c, query)
	if err != nil {
		return time.Duration(0), err
	}
	if plan == nil {
		return time.Duration(0), GoDBError{ParseError, "Plan was nil"}
	}
	if qType != IteratorType {
		return time.Duration(0), GoDBError{ParseError, "Plan is not of iterator type"}
	}
	desc := plan.Descriptor()
	if desc == nil {
		return time.Duration(0), GoDBError{ParseError, "Descriptor was nil"}
	}
	tid := NewTID()
	iter, err := plan.Iterator(tid)
	if err != nil {
		return time.Duration(0), err
	}

	// Run once to collect timing information
	start := time.Now()
	for {
		tup, err := iter()

		// Comment out this check while actually
		// benchmarking, here right now for debugging help
		if err != nil {
			return time.Duration(0), err
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
		timing_csv, err := os.OpenFile(timing_csv_path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return end, GoDBError{OSError, err.Error()}
		}
		fmt.Fprintf(timing_csv, "%s, %v\n", queryName, end)

		outfile_csv, err := os.OpenFile(output_csv_path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return end, GoDBError{OSError, err.Error()}
		}
		fmt.Fprintf(outfile_csv, "%s\n", plan.Descriptor().HeaderString(false))
		iter, err = plan.Iterator(tid)
		if err != nil {
			return time.Duration(0), err
		}
		for {
			tup, err := iter()
			if err != nil {
				return end, err
			}
			if tup == nil {
				break
			}
			fmt.Fprintf(outfile_csv, "%s\n", tup.PrettyPrintString(false))
		}
	}

	return end, nil
}

func RunWikiArticleQuery(query string, c *Catalog, colNo int, bp *BufferPool) ([]int64, error) {
	qType, plan, err := Parse(c, query)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, GoDBError{ParseError, "Plan was nil"}
	}
	if qType != IteratorType {
		return nil, GoDBError{ParseError, "Plan is not of iterator type"}
	}
	desc := plan.Descriptor()
	if desc == nil {
		return nil, GoDBError{ParseError, "Descriptor was nil"}
	}
	tid := NewTID()
	iter, err := plan.Iterator(tid)
	if err != nil {
		return nil, err
	}

	var result []int64
	// Run once to collect timing information
	for {
		tup, err := iter()

		// Comment out this check while actually
		// benchmarking, here right now for debugging help
		if err != nil {
			return nil, err
		}
		if tup == nil {
			break
		}
		result = append(result, tup.Fields[colNo].(IntField).Value)
	}

	bp.CommitTransaction(tid)

	return result, nil
}
