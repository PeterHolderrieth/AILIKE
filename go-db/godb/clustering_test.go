package godb

import (
	"fmt"
	"os"
	"testing"
)

const VerboseClusterTests = false

func getSquareCornerEmb() ([]EmbeddingType, []recordID) {
	embeddings := []EmbeddingType{EmbeddingType{1.0, -1.0},
		EmbeddingType{-1.0, 1.0},
		EmbeddingType{1.0, 1.5},
		EmbeddingType{-2.0, -1.0},
		EmbeddingType{-1.0, 1.0},
		EmbeddingType{-1.0, 3.0},
		EmbeddingType{-1.0, -2.0},
		EmbeddingType{-1.0, -1.0}}
	record_ids := []recordID{0, 1, 2, 3, 4, 5, 6, 7}
	return embeddings, record_ids
}

type SliceEmbeddingOperator struct {
	Slice     []EmbeddingType
	RecordIDs []recordID
}

func (sOp *SliceEmbeddingOperator) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	idx := 0

	return func() (*Tuple, error) {
		if idx >= len(sOp.Slice) {
			return nil, nil
		} else {
			newEmb := sOp.Slice[idx]
			rid := sOp.RecordIDs[idx]
			newField := EmbeddedStringField{Value: "(text)", Emb: newEmb}
			idx += 1
			tdesc := sOp.Descriptor()
			return &Tuple{Desc: *tdesc, Fields: []DBValue{newField}, Rid: rid}, nil
		}
	}, nil
}

func (sOp *SliceEmbeddingOperator) Descriptor() *TupleDesc {
	tdesc := TupleDesc{Fields: []FieldType{FieldType{Fname: "Embedding", TableQualifier: "", Ftype: EmbeddedStringType}}}
	return &tdesc
}

func GetSimpleGetterFunc(columnName string) func(t *Tuple) (*EmbeddingType, error) {
	ftype := FieldType{Fname: columnName}
	return func(t *Tuple) (*EmbeddingType, error) {
		idx, err := findFieldInTd(ftype, &t.Desc)
		if err != nil {
			return nil, err
		}
		field := t.Fields[idx]
		embField := field.(EmbeddedStringField)
		return &embField.Emb, nil
	}
}

func getSliceOperator() SliceEmbeddingOperator {
	embList, recordIDList := getSquareCornerEmb()
	return SliceEmbeddingOperator{Slice: embList, RecordIDs: recordIDList}
}

func TestGetSliceOperator(t *testing.T) {
	operator := getSliceOperator()
	tdesc := operator.Descriptor()
	getterFunc := GetSimpleGetterFunc(tdesc.Fields[0].Fname)
	iterator, err := operator.Iterator(NewTID())
	if err != nil {
		t.Errorf("Error.")
	}
	itCounter := 0
	for newTuple, _ := iterator(); newTuple != nil; newTuple, _ = iterator() {
		if VerboseClusterTests {
			fmt.Println("newTuple: ", newTuple)
			embedding, _ := getterFunc(newTuple)
			fmt.Println("Embedding: ", embedding)
		}
		itCounter += 1
	}
	if itCounter != 8 {
		t.Fatalf("Expected 8 tuples but got different number.")
	}
}

func TestNewClustering(t *testing.T) {

	embeddings, record_ids := getSquareCornerEmb()
	clustering := newClustering(4, 2, true)

	if clustering.NCentroids() != 0 {
		t.Fatalf("Expected zero clusters upon initialisation.")
	}

	for idx := 0; idx < len(embeddings); idx++ {
		_, _, err := clustering.addRecordToClustering(record_ids[idx], &embeddings[idx])
		if err != nil {
			t.Fatalf(err.Error())
		}
		if clustering.NCentroids() != min(idx+1, 4) {
			t.Fatalf("Expected %d clusters upon initialisation.", idx)
		}
	}
	if VerboseClusterTests {
		fmt.Println("***************")
		fmt.Println("Found clustering :")
		clustering.Print()
		fmt.Println("***************")
	}
	clustering.updateAutomaticAllCentroidVectors()
	if VerboseClusterTests {
		fmt.Println("***************")
		fmt.Println("Resulting clustering :")
		clustering.Print()
		fmt.Println("***************")
	}

	clustering.deleteAllMembers()
	if VerboseClusterTests {
		fmt.Println("***************")
		fmt.Println("Deleted all members:")
		clustering.Print()
		fmt.Println("***************")
	}
}

func TestKMeansClusteringSmall(t *testing.T) {
	operator := getSliceOperator()
	tdesc := operator.Descriptor()
	getterFunc := GetSimpleGetterFunc(tdesc.Fields[0].Fname)
	clustering, err := KMeansClustering(&operator, 4, 2, 10, 1.0, getterFunc, true)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if VerboseClusterTests {
		fmt.Println("FOUND CLUSTERING: ")
		clustering.Print()
	}
}

func GetTestHeapFileIterator() (*HeapFile, *BufferPool, error) {

	resetFile := false
	bp := NewBufferPool(1000)
	td := &TupleDesc{Fields: []FieldType{
		FieldType{Fname: "tweet_id", Ftype: IntType},
		FieldType{Fname: "sentiment", Ftype: StringType},
		FieldType{Fname: "content", Ftype: EmbeddedStringType}}}

	if resetFile {

		err := os.Remove("kmeans_test.dat")
		if err != nil {
			panic(err.Error())
		}
		hf, err := NewHeapFile("kmeans_test.dat", td, bp)
		if err != nil {
			return nil, nil, err
		}

		f, err := os.Open("../../data/tweets/tweets_mini.csv")
		if err != nil {
			return nil, nil, err
		}
		err = hf.LoadFromCSV(f, true, ",", false)
		if err != nil {
			return nil, nil, err
		}
		return hf, bp, nil

	} else {
		hf, err := NewHeapFile("kmeans_test.dat", td, bp)
		if err != nil {
			return nil, nil, err
		}
		return hf, bp, nil
	}
}

func (c *Clustering) SampleHeapFileClusteringPrint(hf *HeapFile, bp *BufferPool, nSamples int) {
	for key, _ := range c.centroidEmbs {
		fmt.Println("Cluster ID: ", key)
		fmt.Println("Members: ")
		//Print records in that cluster:
		count := 0
		for _, z := range c.clusterMemb[key] {
			hrid := z.rid.(heapRecordId)
			page, err := bp.GetPage(hf, hrid.pageNo, NewTID(), ReadPerm)
			if err != nil {
				panic(err.Error())
			}
			hpage := (*page).(*heapPage)
			tuple := hpage.records[hrid.slotNo]
			fmt.Println(tuple.PrettyPrintString(false))
			if count > nSamples {
				break
			}
			count++
		}
		fmt.Println("Cluster quality: ", c.sumClusterDist[key])
	}
	fmt.Println("Total distance to centroids: ", c.TotalDist())
	fmt.Println("")
}

func TestKMeansHeapFile(t *testing.T) {
	hfile, bp, err := GetTestHeapFileIterator()
	if err != nil {
		t.Fatalf(err.Error())
	}
	getterFunc := GetSimpleGetterFunc("content")
	clustering, err := KMeansClustering(hfile, 1000, TextEmbeddingDim, 1, 1.0, getterFunc, false)

	if VerboseClusterTests {
		clustering.SampleHeapFileClusteringPrint(hfile, bp, 10)
	}
}
