package godb

import "fmt"

func vectorIndexExists(field FieldType, c *Catalog) bool {
	// TODO: check if a vector index for a given table/column exist
	return true
}

type VectorIndex struct {
	indexField  FieldType
	queryVector ConstExpr
	heapFile    HeapFile // tempory hack to continue doing heap scans until vector index implemented
	limitTups   Expr     // number of tuples to limit to
	ascending   bool     // whether to order by most or least similar
}

// Create an
func NewVectorIndex(heapFile HeapFile, limit Expr, indexField FieldType, queryVector ConstExpr, ascending bool) (*VectorIndex, error) {
	// TODO: add validation on inputs
	return &VectorIndex{indexField, queryVector, heapFile, limit, ascending}, nil
}

func (v *VectorIndex) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	return v.heapFile.Iterator(tid)
}

func (v *VectorIndex) Descriptor() *TupleDesc {
	return v.heapFile.Descriptor()
}

func (v *VectorIndex) PrettyPrint() string {
	limitVal, err := v.limitTups.EvalExpr(nil) // using nil since limitTups is a ConstExpr that does not depend on tuple
	if err != nil {
		panic("Cannot evaluate limit within VectorIndex PrettyPrint.")
	}
	limit := limitVal.(IntField).Value
	query := v.queryVector.val.(EmbeddedStringField).Value
	var orderString string = "descending"
	if v.ascending {
		orderString = "ascending"
	}
	return fmt.Sprintf("{column: %v, table: %v, limit: %v, %v, query: %v}", v.indexField.Fname, v.indexField.TableQualifier, limit, orderString, query)
}
