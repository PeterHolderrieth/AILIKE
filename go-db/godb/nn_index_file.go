package godb

import (
	"os"
)

// TupleDesc for the heap file that stores the maping from vectors to heapRecordIds.
// heapRecordIds are composed from filenames, pageNo, slotNo; we already know
// the table's filename, so we don't need to store it in the dataHeapFile, though we could.
var dataDesc = TupleDesc{Fields: []FieldType{
	{Fname: "vector", Ftype: VectorFieldType},
	{Fname: "tablePageNo", Ftype: IntType},
	{Fname: "slotNo", Ftype: IntType},
}}

// TupleDesc for the heap file that stores the maping from centroids to index pageNos.
var centroidDesc = TupleDesc{Fields: []FieldType{
	{Fname: "vector", Ftype: VectorFieldType},
	{Fname: "centroidId", Ftype: IntType},
}}

// TupleDesc for the heap file that stores the maping from centroids to index pageNos.
var mappingDesc = TupleDesc{Fields: []FieldType{
	{Fname: "centroidId", Ftype: IntType},
	{Fname: "indexPageNo", Ftype: IntType},
}}

// NNIndexFile provides a nearest-neighbor index for a given table stored within a HeapFile.
type NNIndexFile struct {
	tableFileName  string // the filename of the table this is an index for
	indexedColName string // the name of the column being indexed; must be EmbeddedString column
	// We use a heapFile to store the vector <-> heapRecordId mappings
	dataHeapFile *HeapFile
	// We use another heap file to store centroid <-> centoidId mappings
	centroidHeapFile *HeapFile
	// We use a third heap file to store centroidId <-> pageNo, where pageNo is a page that contains
	// rows for the given centroid within the dataHeapFile
	mappingHeapFile *HeapFile
}

// Create a NnIndexFile.
// Parameters
// - fromTableFile: the filename for the HeapFile for the Table that this NN index is for.
// - indexedColName: the column in the table that is indexed
// - fromDataFile: the backing file for this index that store the vector <-> heapRecordId mapping
// - fromCentroidFile: the backing file for this index that stores the centroid <-> pageNo mapping
// - bp: the BufferPool that is used to store pages read from this index
// May return an error if the file cannot be opened or created.
func NewNNIndexFileFile(tableFileName string, indexedColName string, fromDataFile string, fromCentroidFile string, fromMappingFile string, bp *BufferPool) (*NNIndexFile, error) {
	dataHeapFile, err := NewHeapFile(fromDataFile, &dataDesc, bp)
	if err != nil {
		return nil, err
	}
	centroidHeapFile, err := NewHeapFile(fromCentroidFile, &centroidDesc, bp)
	if err != nil {
		return nil, err
	}
	mappingHeapFile, err := NewHeapFile(fromMappingFile, &mappingDesc, bp)
	if err != nil {
		return nil, err
	}
	return &NNIndexFile{tableFileName, indexedColName, dataHeapFile, centroidHeapFile, mappingHeapFile}, nil
}

// Given an embedding, return an iterator that returns the [centroidId, pageNo] pairs ordered by distance between the centroid
// and the embedding; multiple rows may have the same centroidId, but different pageNos.
// Parameters
// - e: the embedding to find the nearest cluster pages for
// - ascending: if true, return the nearest cluster pages first; if false, return the farthest cluster pages first
// - tid: the transaction id
// - p (optional): the number of centroids to visit; if p is negative, visit all centroids
func (f *NNIndexFile) getCentroidPageNoIterator(e EmbeddedStringField, ascending bool, tid TransactionID, p int) (func() ([2]int, error), error) {
	var centroidIdFieldExpr Expr = &FieldExpr{FieldType{Fname: "centroidId", Ftype: IntType}}
	var fe Expr = &FieldExpr{FieldType{Fname: "vector", Ftype: VectorFieldType}}
	var ce Expr = &ConstExpr{e, EmbeddedStringType}
	// TODO: support multiple distance metrics
	var ailikeExpr Expr = &FuncExpr{"ailike_vec", []*Expr{&fe, &ce}}
	proj, err := NewProjectOp([]Expr{centroidIdFieldExpr, ailikeExpr}, []string{"centroidId", "dist"}, false, f.centroidHeapFile)
	if err != nil {
		return nil, err
	}
	var distFieldExpr Expr = &FieldExpr{FieldType{Fname: "dist", Ftype: IntType}}
	orderby, err := NewOrderBy([]Expr{distFieldExpr}, proj, []bool{true})
	if err != nil {
		return nil, err
	}
	var topOp Operator = orderby
	if p > 0 {
		topOp = NewLimitOp(&ConstExpr{IntField{int64(p)}, IntType}, orderby)
	}
	join, err := NewIntJoin(topOp, centroidIdFieldExpr, f.mappingHeapFile, centroidIdFieldExpr, 10)
	if err != nil {
		return nil, err
	}

	joinIter, err := join.Iterator(tid)
	if err != nil {
		return nil, err
	}

	var centoidId int
	var pageNo int
	return func() ([2]int, error) {
		row, err := joinIter()
		if err != nil {
			return [2]int{-1, -1}, err
		}
		if row != nil {
			centoidId = int(row.Fields[0].(IntField).Value)
			pageNo = int(row.Fields[3].(IntField).Value)
			return [2]int{centoidId, pageNo}, nil
		}
		return [2]int{-1, -1}, nil
	}, nil
}

// Finds a page for the nearest centroid with room for a new record, or creates a new page for that centroid if needed
func (f *NNIndexFile) insertTuple(t *Tuple, tid TransactionID) error {
	if t.Rid.(heapRecordId).fileName != f.tableFileName {
		return GoDBError{IncompatibleTypesError, "Index does not match table of tuple."}
	}

	var colIndex int = -1
	for i, field := range t.Desc.Fields {
		if field.Fname == f.indexedColName {
			colIndex = i
		}
	}
	if colIndex == -1 {
		return GoDBError{IncompatibleTypesError, "Given tuple does not contain indexed column."}
	}

	var embeddingField EmbeddedStringField = t.Fields[colIndex].(EmbeddedStringField) // TODO: more type checking?
	centroidPageNoIter, err := f.getCentroidPageNoIterator(embeddingField, true, tid, 1)
	if err != nil {
		return err
	}
	var inserted bool = false
	var centroidId int
	var pageNo int
	var dt Tuple = Tuple{Desc: dataDesc, Fields: []DBValue{VectorField{embeddingField.Emb}, IntField{int64(t.Rid.(heapRecordId).pageNo)}, IntField{int64(t.Rid.(heapRecordId).slotNo)}}}

	for row, err := centroidPageNoIter(); row[0] != -1; row, err = centroidPageNoIter() {
		if err != nil {
			return err
		}
		centroidId = row[0]
		pageNo = row[1]
		err = f.dataHeapFile.insertTupleIntoPage(&dt, pageNo, tid)
		if err != nil {
			if err.(GoDBError).code == PageFullError {
				continue
			}
			return err
		} else {
			// successfully inserted tuple into a page
			inserted = true
			break
		}
	}
	if !inserted {
		newPageNo, err := f.dataHeapFile.insertTupleIntoNewPage(&dt, tid)
		if err != nil {
			return err
		}
		newMappingTuple := &Tuple{Desc: mappingDesc, Fields: []DBValue{IntField{int64(centroidId)}, IntField{int64(newPageNo)}}}
		err = f.mappingHeapFile.insertTuple(newMappingTuple, tid)
		if err != nil {
			return err
		}
	}
	return nil
}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *NNIndexFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// TODO
	return nil
}

// Creates a nearest neighbor index for the given heap file column with nClusters.
// An NNIndexFile is stored by 3 heap files under the hood: a data file, centroid file, and mapping file.
//
// NOTE: currently, constructing an index cannot be run cuncurrently with other transactions
//
// Parameters:
// - hfile: the heap file to create an index for
// - indexedColName: the column in hfile that the index is for
// - nClusters: the number of clusters to create
// - dataFileName: the filename to save the index's dataHeapFile under
// - centroidFileName: the filename to save the index's centroidHeapFile under
// - mappingFileName: the filename to save the index's mappingHeapFile under
// - bp: the buffer pool to use
func ConstructNNIndexFileFromHeapFile(hfile *HeapFile, indexedColName string, nClusters int, dataFileName string, centroidFileName string, mappingFileName string, bp *BufferPool) (*NNIndexFile, error) {
	tid := NewTID()

	//Create clustering
	getterFunc := GetSimpleGetterFunc(indexedColName)
	clustering, err := KMeansClustering(hfile, nClusters, TextEmbeddingDim,
		MaxIterKMeans, DeltaThrKMeans, getterFunc, false)
	if err != nil {
		return nil, err
	}

	//Create data file
	os.Remove(dataFileName)
	dataHeapFile, err := NewHeapFile(dataFileName, &dataDesc, bp)
	if err != nil {
		return nil, err
	}

	//Create centroid file
	os.Remove(centroidFileName)
	centroidHeapFile, err := NewHeapFile(centroidFileName, &centroidDesc, bp)
	if err != nil {
		return nil, err
	}

	//Create mapping file
	os.Remove(mappingFileName)
	mappingHeapFile, err := NewHeapFile(mappingFileName, &mappingDesc, bp)
	if err != nil {
		return nil, err
	}
	nnif := &NNIndexFile{hfile.fileName, indexedColName, dataHeapFile, centroidHeapFile, mappingHeapFile}

	// allow stealing pages from buffer pool
	// NOTE: cannot create indexes cuncurrently with other transactions
	bp.steal = true

	// clustering.Print()
	//Insert all centroids and elements into the data file
	for centroidID, centroid := range clustering.centroidEmbs {
		centroidTuple := Tuple{centroidDesc, []DBValue{VectorField{*centroid}, IntField{int64(centroidID)}}, nil}
		err = nnif.centroidHeapFile.insertTuple(&centroidTuple, tid)
		if err != nil {
			return nil, err
		}
	}

	iter, err := hfile.Iterator(tid)
	if err != nil {
		return nil, err
	}
	for t, err := iter(); t != nil || err != nil; t, err = iter() {
		if err != nil {
			return nil, err
		}
		err = nnif.insertTuple(t, tid)
		if err != nil {
			return nil, err
		}
	}
	bp.CommitTransaction(tid)

	bp.steal = false
	return nnif, nil
}
