package godb

import "fmt"

// Function to be queried in parser to check whether a given heap file
// has an index for a specific column
func nnIndexExists(field FieldType, c *Catalog) (bool, error) {
	tableName := field.TableQualifier
	if t, ok := c.tableMap[tableName]; ok && t != nil {
		dbFile, err := c.GetTable(tableName)
		if err != nil {
			return false, GoDBError{NoSuchTableError, fmt.Sprintf("no table '%s' found", tableName)}
		}
		hf, ok := dbFile.(*HeapFile)
		if !ok {
			return false, GoDBError{NoSuchTableError, fmt.Sprintf("Issue reading table '%s'", tableName)}
		}
		if index := getIndexForField(field, hf); index != nil {
			return true, nil
		}
	}
	return false, nil
}

// Get index for field
func getIndexForField(field FieldType, hf *HeapFile) *NNIndexFile {
	colName := field.Fname
	if index, ok := hf.indexes[colName]; ok && index != nil {
		return index
	}
	return nil
}

type NNScan struct {
	indexField     FieldType
	queryEmbedding EmbeddedStringField
	heapFile       *HeapFile // tempory hack to continue doing heap scans until vector index implemented
	nnIndexFile    *NNIndexFile
	limitNo        int  // number of tuples to limit to
	ascending      bool // whether to order by most or least similar
}

// Create an
func NewNNScan(heapFile *HeapFile, limit Expr, indexField FieldType, queryExpr ConstExpr, ascending bool) (*NNScan, error) {
	index := getIndexForField(indexField, heapFile)
	if index == nil {
		return nil, GoDBError{NoSuchTableError, fmt.Sprintf("No index found for field '%s'", indexField.Fname)}
	}

	if queryExpr.constType != EmbeddedStringType {
		return nil, GoDBError{IncompatibleTypesError, "Query expression must be an embedded string"}
	}
	queryEmbedding := queryExpr.val.(EmbeddedStringField)

	limitVal, err := limit.EvalExpr(nil)
	if err != nil {
		panic("Cannot evaluate limit within NNScan.")
	}
	limitNo := int(limitVal.(IntField).Value)

	return &NNScan{indexField, queryEmbedding, heapFile, index, limitNo, ascending}, nil
}

func (v *NNScan) GetNumberOfProbes() int {
	nCentroids := v.nnIndexFile.NCentroids()
	nTuples := v.heapFile.ApproximateNumTuples()
	avgClusterSize := nTuples / nCentroids
	return v.limitNo/avgClusterSize + DefaultProbe
}


func (v *NNScan) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: test strategy for number of probes for large limits
	nProbes := v.GetNumberOfProbes()
	fmt.Println("Default Probe = ", DefaultProbe, "Num Probes = ", nProbes)

	centroidPageIter, err := v.nnIndexFile.getCentroidPageNoIterator(v.queryEmbedding, v.ascending, tid, nProbes)
	if err != nil {
		return nil, err
	}
	var indexTupleIter func() (*Tuple, error) = func() (*Tuple, error) {
		return nil, nil
	}
	var hrid heapRecordId
	return func() (*Tuple, error) {
		var t *Tuple
		t, err := indexTupleIter()
		if err != nil {
			return nil, err
		}
		for t == nil {
			// for centroidPageNoPair, err := centroidPageIter(); t == nil && centroidPageNoPair[1] != -1; centroidPageNoPair, err = centroidPageIter() {
			centroidPageNoPair, err := centroidPageIter()
			if err != nil {
				return nil, err
			}
			if centroidPageNoPair[1] == -1 {
				return nil, nil
			}
			nextPageNo := centroidPageNoPair[1]
			nextIndexPage, err := v.nnIndexFile.dataHeapFile.getHeapPage(nextPageNo, tid, ReadPerm)
			if err != nil {
				return nil, err
			}
			indexTupleIter = nextIndexPage.tupleIter()
			t, err = indexTupleIter()
			if err != nil {
				return nil, err
			}
		}
		if v.nnIndexFile.clustered {
			return t, nil
		}
		hrid = heapRecordId{v.heapFile.fileName, int(t.Fields[1].(IntField).Value), int(t.Fields[2].(IntField).Value)}
		nt, err := v.heapFile.findTuple(hrid, tid)
		if err != nil {
			return nil, err
		}
		return nt, nil

	}, nil
}

func (v *NNScan) Descriptor() *TupleDesc {
	return v.heapFile.Descriptor()
}

func (v *NNScan) PrettyPrint() string {
	query := v.queryEmbedding.Value
	var orderString string = "descending"
	if v.ascending {
		orderString = "ascending"
	}
	return fmt.Sprintf("{clustered: %v, column: %v, table: %v, limit: %v, %v, query: %v}", v.nnIndexFile.clustered, v.indexField.Fname, v.indexField.TableQualifier, v.limitNo, orderString, query)
}
