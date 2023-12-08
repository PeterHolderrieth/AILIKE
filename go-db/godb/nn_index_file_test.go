package godb

import (
	"fmt"
	"testing"
)

func TestConstructIndexUnclustered(t *testing.T) {
	var num_records int = 100 // tweets_test has 100 records in it
	hfile, bp, err := MakeTestDatabaseFromCsv("tweets_test", "../../data/tweets/tweets_test.csv", 10)
	if err != nil {
		t.Fatalf(err.Error())
	}

	var numClusters int = 10
	ifile, err := ConstructNNIndexFileFromHeapFile(hfile, "content", numClusters, false, ".", "tweets_test", bp)
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

func TestConstructIndexClustered(t *testing.T) {
	var num_records int = 100 // tweets_test has 100 records in it
	hfile, bp, err := MakeTestDatabaseFromCsv("tweets_test_clustered", "../../data/tweets/tweets_test.csv", 10)
	if err != nil {
		t.Fatalf(err.Error())
	}
	tid := NewTID()
	fmt.Println("test heap file original: ", hfile.fileName, hfile.NumTuples(tid), hfile.NumPages())

	var numClusters int = 10
	ifile, err := ConstructNNIndexFileFromHeapFile(hfile, "content", numClusters, true, ".", "tweets_test", bp)
	if err != nil {
		t.Fatalf("failed to construct index file, %s", err.Error())
	}

	iter, _ := ifile.centroidHeapFile.Iterator(tid)
	centroidCount := 0
	for t, _ := iter(); t != nil; t, _ = iter() {
		centroidCount++
	}
	if centroidCount != numClusters {
		t.Fatalf("expected %d centroids, got %d", numClusters, centroidCount)
	}

	iter, _ = ifile.dataHeapFile.Iterator(tid)
	dataCount := 0
	for t, _ := iter(); t != nil; t, _ = iter() {
		dataCount++
	}
	fmt.Println(bp.steal)
	fmt.Println("test heap  file: ", hfile.fileName, hfile.NumTuples(tid), hfile.NumPages())
	fmt.Println("test index data: ", ifile.dataHeapFile.fileName, ifile.dataHeapFile.NumTuples(tid), ifile.dataHeapFile.NumPages())
	fmt.Println("test heap  file: ", hfile.fileName, hfile.NumTuples(tid), hfile.NumPages())
	fmt.Println("test index data: ", ifile.dataHeapFile.fileName, ifile.dataHeapFile.NumTuples(tid), ifile.dataHeapFile.NumPages())
	if dataCount != num_records {
		t.Fatalf("expected %d records, got %d", num_records, dataCount)
	}
}
