package godb

import (
	"fmt"
	"testing"
)

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
	fmt.Println("operator.Descriptor(): ", tdesc)
	iterator, err := operator.Iterator(NewTID())
	if err != nil {
		t.Errorf("Error.")
	}
	itCounter := 0
	verbose := true
	for newTuple, _ := iterator(); newTuple != nil; newTuple, _ = iterator() {
		if verbose {
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
	clustering := newClustering(4, 2)

	if clustering.NCentroids() != 0 {
		t.Fatalf("Expected zero clusters upon initialisation.")
	}

	for idx := 0; idx < len(embeddings); idx++ {
		fmt.Println("Adding records: ", record_ids[idx])
		fmt.Println("adding embedding: ", embeddings[idx])
		err := clustering.addRecordToClustering(record_ids[idx], &embeddings[idx])
		if err != nil {
			t.Fatalf(err.Error())
		}
		if clustering.NCentroids() != min(idx+1, 4) {
			t.Fatalf("Expected %d clusters upon initialisation.", idx)
		}
	}
	verbose := true
	if verbose {
		fmt.Println("***************")
		fmt.Println("Found clustering :")
		clustering.Print()
		fmt.Println("***************")
	}
	clustering.updateAllCentroidVectors()
	if verbose {
		fmt.Println("***************")
		fmt.Println("Resulting clustering :")
		clustering.Print()
		fmt.Println("***************")
	}

	clustering.deleteAllMembers()
	if verbose {
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
	clustering, err := KMeansClustering(&operator, 4, 2, 10, 1.0, getterFunc)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println("FOUND CLUSTERING: ")
	clustering.Print()
}
