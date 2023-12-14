package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child: child, limitTups: lim}
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, ailikeError{MalformedDataError, "LimitOp child Iterator unexpectedly ni."}
	}

	limitVal, err := l.limitTups.EvalExpr(nil) // using nil since limitTups is a ConstExpr that does not depend on tuple
	if err != nil {
		return nil, err
	}
	limit := limitVal.(IntField).Value
	var i int64 = 0
	return func() (*Tuple, error) {
		if i < limit {
			t, err := childIter()
			if err != nil {
				return nil, err
			}
			i++
			return t, nil
		}
		return nil, nil
	}, nil
}
