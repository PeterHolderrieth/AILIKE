package godb

import (
	"fmt"
	"testing"
)

func TestGetWikiElement(t *testing.T) {
	wikielement, err := getWikiElement(0)
	fmt.Println(wikielement)
	if err != nil {
		t.Fatalf(err.Error())
	}
}
func TestConstructWikiHeapFile(t *testing.T) {
	bp := NewBufferPool(100)
	hfile, err := ConstructWikiHeapFile("test_wiki", bp, true, 10)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println("Number of tuples in heap file: ", hfile.ApproximateNumTuples())
	tid := NewTID()
	hfileIter, err := hfile.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for tuple, err := hfileIter(); (tuple != nil) && (err == nil); tuple, err = hfileIter() {
		fmt.Println(tuple.PrettyPrintString(true))
	}
	if err != nil {
		t.Fatalf(err.Error())
	}
}

const recreateWiki = false

func TestConstructFullWikiHeapFile(t *testing.T) {
	if recreateWiki {
		bp := NewBufferPool(100)
		hfile, err := ConstructWikiHeapFile("wiki_full", bp, true, 6458665)
		if err != nil {
			t.Fatalf(err.Error())
		}
		fmt.Println("Number of tuples in heap file: ", hfile.ApproximateNumTuples())
	}
}
