package godb

import (
	"testing"
)

func TestConstructIndex(t *testing.T) {
	var num_records int = 100 // tweets_test has 100 records in it
	hfile, bp, err := MakeTestDatabaseFromCsv("tweets_test", "../../data/tweets/tweets_test.csv", 10)
	if err != nil {
		t.Fatalf(err.Error())
	}

	var numClusters int = 10
	ifile, err := ConstructNNIndexFileFromHeapFile(hfile, "content", numClusters, "index_data_test.dat", "index_centroids_test.dat", "index_mapping_test.dat", bp)
	if err != nil {
		t.Fatalf("failed to construct index file, %s", err.Error())
	}

	iter, _ := ifile.centroidHeapFile.Iterator(NewTID())
	centroidCount := 0
	for t, _ := iter(); t != nil; t, _ = iter() {
		centroidCount++
	}
	if centroidCount != numClusters {
		t.Fatalf("expected %d centroids, got %d", numClusters, centroidCount)
	}

	iter, _ = ifile.dataHeapFile.Iterator(NewTID())
	dataCount := 0
	for t, _ := iter(); t != nil; t, _ = iter() {
		dataCount++
	}
	if dataCount != num_records {
		t.Fatalf("expected %d records, got %d", num_records, dataCount)
	}
}
