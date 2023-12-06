package godb

import (
	"fmt"
	"testing"
)

func TestBenchmarkingInfra(t *testing.T) {
	var config = BenchMetaData{
		catalog:   "catalog_text.txt",
		dbDir:     "./",
		outputDir: "./benchmark_results",
		bpSize:    10,
		save:      true}
	time, err := BenchmarkingInfra("test", "select biography, 'doctor' ailike biography from t_text", config)
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	fmt.Println("Time taken = ", time)
}
