package godb

type DeleteOp struct {
	file  DBFile
	child Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{file: deleteFile, child: child}

}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	ft := FieldType{Fname: "count", TableQualifier: "", Ftype: IntType}
	fts := []FieldType{ft}
	return &TupleDesc{Fields: fts}
}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, ailikeError{MalformedDataError, "DeleteOp child Iterator unexpectedly nil."}
	}
	if !dop.child.Descriptor().equals(dop.file.Descriptor()) {
		return nil, ailikeError{TypeMismatchError, "Trying to delete tuples with wrong type from table."}
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
			err := dop.file.deleteTuple(t, tid)
			if err != nil {
				return nil, err
			}
			count++
		}
		complete = true
		countField := IntField{int64(count)}
		return &Tuple{Desc: *dop.Descriptor(), Fields: []DBValue{countField}, Rid: nil}, nil
	}, nil
}
