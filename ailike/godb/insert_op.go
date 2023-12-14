package godb

type InsertOp struct {
	file  DBFile
	child Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{file: insertFile, child: child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	ft := FieldType{Fname: "count", TableQualifier: "", Ftype: IntType}
	fts := []FieldType{ft}
	return &TupleDesc{Fields: fts}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := iop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, ailikeError{MalformedDataError, "InsertOp child Iterator unexpectedly nil."}
	}
	if !iop.child.Descriptor().equals(iop.file.Descriptor()) {
		return nil, ailikeError{TypeMismatchError, "Trying to insert tuples with wrong type into table."}
	}
	complete := false

	return func() (*Tuple, error) {
		if complete {
			return nil, nil
		}
		count := 0
		for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
			if err != nil {
				return nil, err
			}
			err := iop.file.insertTuple(t, tid)
			if err != nil {
				return nil, err
			}
			count++
		}
		complete = true
		countField := IntField{int64(count)}
		return &Tuple{Desc: *iop.Descriptor(), Fields: []DBValue{countField}, Rid: nil}, nil
	}, nil
}
