package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	distinct     bool
	child        Operator
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	if len(selectFields) != len(outputNames) {
		// TODO: Introduce a new error type; this is not quite right.
		return nil, GoDBError{MalformedDataError, "selectFields and outputNames have different lengths."}
	}
	if child == nil {
		return nil, GoDBError{MalformedDataError, "NewProjectOp child pointer is nil."}
	}
	return &Project{selectFields, outputNames, distinct, child}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	fs := make([]FieldType, len(p.selectFields))
	for i, expr := range p.selectFields {
		et := expr.GetExprType()
		fs[i] = FieldType{Fname: p.outputNames[i], TableQualifier: et.TableQualifier, Ftype: et.Ftype}
	}
	return &TupleDesc{Fields: fs}
}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "ProjectOp child Iterator unexpectedly nil."}
	}
	seen := make(map[any]bool)

	return func() (*Tuple, error) {
		for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
			if err != nil {
				return nil, err
			}

			fields := make([]DBValue, len(p.selectFields))
			for i, expr := range p.selectFields {
				value, err := expr.EvalExpr(t)
				if err != nil {
					return nil, err
				}
				fields[i] = value
			}
			pt := &Tuple{Desc: *p.Descriptor(), Fields: fields, Rid: nil}
			tk := pt.tupleKey()
			if _, ok := seen[tk]; !ok || !p.distinct {
				seen[tk] = true
				return pt, nil
			}
		}
		return nil, nil
	}, nil
}
