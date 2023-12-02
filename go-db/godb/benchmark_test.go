package godb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

type BenchMetaData struct {
	catalog string
	path    string
	bpSize  int
	outputFile string
	save 			bool
}

func BenchmarkingInfra(query string, config BenchMetaData)  (time.Duration, error) {
	bp := NewBufferPool(config.bpSize)
	c, err := NewCatalogFromFile(config.catalog, bp, config.path)
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
	for{
		tup, err := iter()

		// Comment out this check while actually  
		// benchmarking, here right now for debugging help
		if err != nil{
			return time.Duration(0), err
		}
		if tup == nil{
			break
		}
	}
	end := time.Since(start)

	// Now save output
	outfile_csv, err := os.OpenFile(config.outputFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return end, GoDBError{OSError, err.Error()}
	}
	if config.save {
		fmt.Fprintf(outfile_csv, "%s\n", plan.Descriptor().HeaderString(false))
		iter, err = plan.Iterator(tid)
		if err != nil {
			return time.Duration(0), err
		}
		for {
			tup, err := iter()
			if err != nil{
				return end, err
			}
			if tup == nil {
				break
			}
			fmt.Fprintf(outfile_csv, "%s\n", tup.PrettyPrintString(false))
		}
	}

	return end, nil;
}

func TestBenchmarkingInfra(t *testing.T) {
	var config = BenchMetaData{
		catalog: "catalog_text.txt",
		path:    "./",
		outputFile: "benchmark_results/test.csv",
		bpSize:  10, 
		save: 	true}
	time, err := BenchmarkingInfra("select biography, 'doctor' ailike biography from t_text", config)
	if err != nil{
		t.Errorf("%s", err.Error())
	}
	fmt.Println("Time taken = ", time)
}
