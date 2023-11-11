package godb

import (
	"sort"
)

type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
	tuples    []Tuple
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{orderBy: orderByFields, child: child, ascending: ascending, tuples: nil}, nil

}

func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// Sort sorts the argument slice according to the less functions passed to OrderedBy.
func (o *OrderBy) Sort(tuples []Tuple) {
	o.tuples = tuples
	sort.Sort(o)
}

// Len is part of sort.Interface.
func (o *OrderBy) Len() int {
	return len(o.tuples)
}

// Swap is part of sort.Interface.
func (o *OrderBy) Swap(i, j int) {
	o.tuples[i], o.tuples[j] = o.tuples[j], o.tuples[i]
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other).
func (o *OrderBy) Less(i, j int) bool {
	p, q := &o.tuples[i], &o.tuples[j]
	for k := 0; k < len(o.orderBy); k++ {
		expr := o.orderBy[k]
		order, err := p.compareField(q, expr)
		if !o.ascending[k] {
			order, err = q.compareField(p, expr)
		}
		if err != nil {
			panic("Error while comparing fields in OrderBy.")
		}
		switch order {
		case OrderedLessThan:
			// p < q, so we have a decision.
			return true
		case OrderedGreaterThan:
			// p > q, so we have a decision.
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons are "equal", so arbitrarily return false
	return false
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "OrderBy child Iterator unexpectedly nil."}
	}

	var sorted bool = false
	ts := make([]Tuple, 0)
	var i int = 0
	var tup Tuple

	return func() (*Tuple, error) {
		if !sorted {
			for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
				if err != nil {
					return nil, err
				}
				ts = append(ts, *t)
			}
			o.Sort(ts)
		}
		sorted = true
		if i < len(ts) {
			tup = ts[i]
			i++
			return &tup, nil
		}
		return nil, nil
	}, nil
}
