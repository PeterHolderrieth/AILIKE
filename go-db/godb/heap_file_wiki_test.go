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
	hfile, err := ConstructWikiHeapFile("test_wiki", bp, true, 10, false)
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

const recreateRandomWiki = true
const recreateWiki = false
const limitRandomWiki = 100000

var fileNameRandomWiki string = fmt.Sprintf("wiki_random_%d", limitRandomWiki)

func TestConstructRandomWikiHeapFile(t *testing.T) {
	if recreateRandomWiki {
		bp := NewBufferPool(100)
		hfile, err := ConstructWikiHeapFile(fileNameRandomWiki, bp, true, limitRandomWiki, true)
		if err != nil {
			t.Fatalf(err.Error())
		}
		fmt.Println("Number of tuples in heap file: ", hfile.ApproximateNumTuples())
	}
}

func TestConstructFullWikiHeapFile(t *testing.T) {
	if recreateWiki {
		bp := NewBufferPool(100)
		hfile, err := ConstructWikiHeapFile("wiki_full", bp, true, 1000000, true)
		if err != nil {
			t.Fatalf(err.Error())
		}
		fmt.Println("Number of tuples in heap file: ", hfile.ApproximateNumTuples())
	}
}
