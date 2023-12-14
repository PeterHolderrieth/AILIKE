package godb

type Aggregator struct {
	// Expressions that when applied to tuples from the child operators,
	// respectively, return the value of the group by key tuple
	groupByFields []Expr

	// Aggregation states that serves as a template as to which types of
	// aggregations in which order are to be computed for every group.
	newAggState []AggState

	child Operator // the child operator for the inputs to aggregate
}

type AggType int

const (
	IntAggregator    AggType = iota
	StringAggregator AggType = iota
)

const DefaultGroup int = 0 // for handling the case of no group-by

// Constructor for an aggregator with a group-by
func NewGroupedAggregator(emptyAggState []AggState, groupByFields []Expr, child Operator) *Aggregator {
	return &Aggregator{groupByFields, emptyAggState, child}
}

// Constructor for an aggregator with no group-by
func NewAggregator(emptyAggState []AggState, child Operator) *Aggregator {
	return &Aggregator{nil, emptyAggState, child}
}

func (a *Aggregator) getGroupByTupleDesc() *TupleDesc {
	if a.groupByFields == nil {
		panic("Called getGroupByTupleDesc on nil groupByFields.")
	}
	gbfs := make([]FieldType, len(a.groupByFields))
	for i, expr := range a.groupByFields {
		gbfs[i] = expr.GetExprType()
	}
	return &TupleDesc{Fields: gbfs}
}

func (a *Aggregator) getAggStateTupleDesc() *TupleDesc {
	var aggtd TupleDesc = *a.newAggState[0].GetTupleDesc()
	for i := 1; i < len(a.newAggState); i++ {
		aggtd = *aggtd.merge(a.newAggState[i].GetTupleDesc())
	}
	return &aggtd
}

// Return a TupleDescriptor for this aggregation. If the aggregator has no group-by, the
// returned descriptor should contain the union of the fields in the descriptors of the
// aggregation states. If the aggregator has a group-by, the returned descriptor will
// additionally start with the group-by fields, and then the aggregation states descriptors
// like that without group-by.
//
// HINT: for groupByFields, you can use [Expr.GetExprType] to get the FieldType
// HINT: use the merge function you implemented for TupleDesc in lab1 to merge the two TupleDescs
func (a *Aggregator) Descriptor() *TupleDesc {
	var aggtd TupleDesc = *a.getAggStateTupleDesc()
	if a.groupByFields != nil {
		gbtd := a.getGroupByTupleDesc()
		aggtd = *gbtd.merge(&aggtd)
	}
	return &aggtd
}

// Aggregate operator implementation: This function should iterate over the results of
// the aggregate. The aggregate should be the result of aggregating each group's tuples
// and the iterator should iterate through each group's result. In the case where there
// is no group-by, the iterator simply iterates through only one tuple, representing the
// aggregation of all child tuples.
func (a *Aggregator) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// the child iterator
	childIter, err := a.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, ailikeError{MalformedDataError, "Aggregator child Iterator unexpectedly nil."}
	}
	// the map that stores the aggregation state of each group
	aggState := make(map[any]*[]AggState)
	if a.groupByFields == nil {
		var newAggState []AggState
		for _, as := range a.newAggState {
			copy := as.Copy()
			if copy == nil {
				return nil, ailikeError{MalformedDataError, "Copy of aggState unexpectedly returned nil."}
			}
			newAggState = append(newAggState, copy)
		}

		aggState[DefaultGroup] = &newAggState
	}
	// the list of group key tuples
	var groupByList []*Tuple
	// the iterator for iterating thru the finalized aggregation results for each group
	var finalizedIter func() (*Tuple, error)
	return func() (*Tuple, error) {
		if finalizedIter == nil {
			// iterates thru all child tuples
			for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
				if err != nil {
					return nil, err
				}

				if a.groupByFields == nil { // adds tuple to the aggregation in the case of no group-by
					for i := 0; i < len(a.newAggState); i++ {
						(*aggState[DefaultGroup])[i].AddTuple(t)
					}
				} else { // adds tuple to the aggregation with grouping
					keygenTup, err := extractGroupByKeyTuple(a, t)
					if err != nil {
						return nil, err
					}

					key := keygenTup.tupleKey()
					if aggState[key] == nil {
						asNew := make([]AggState, len(a.newAggState))
						aggState[key] = &asNew
						groupByList = append(groupByList, keygenTup)
					}

					addTupleToGrpAggState(a, t, aggState[key])
				}
			}

			// build the iterator for iterating thru the finalized aggregation results for each group
			if a.groupByFields == nil {
				tup := &Tuple{Desc: TupleDesc{}}
				for i := 0; i < len(a.newAggState); i++ {
					newTup := (*aggState[DefaultGroup])[i].Finalize()
					tup = joinTuples(tup, newTup)
				}
				finalizedIter = func() (*Tuple, error) { return nil, nil }
				return tup, nil
			} else {
				finalizedIter = getFinalizedTuplesIterator(a, groupByList, aggState)
			}
		}
		return finalizedIter()
	}, nil
}

// Given a tuple t from a child iteror, return a tuple that identifies t's group.
// The returned tuple should contain the fields from the groupByFields list
// passed into the aggregator constructor.  The ith field can be extracted
// from the supplied tuple using the EvalExpr method on the ith expression of
// groupByFields.
// If there is any error during expression evaluation, return the error.
func extractGroupByKeyTuple(a *Aggregator, t *Tuple) (*Tuple, error) {
	fields := make([]DBValue, len(a.groupByFields))
	for i, expr := range a.groupByFields {
		value, err := expr.EvalExpr(t)
		if err != nil {
			return nil, err
		}
		fields[i] = value
	}
	return &Tuple{Desc: *a.getGroupByTupleDesc(), Fields: fields, Rid: nil}, nil
}

// Given a tuple t from child and (a pointer to) the array of partially computed aggregates
// grpAggState, add t into all partial aggregations using the [AggState AddTuple] method.
// If any of the array elements is of grpAggState is null (i.e., because this is the first
// invocation of this method, create a new aggState using aggState.Copy() on appropriate
// element of the a.newAggState field and add the new aggState to grpAggState.
func addTupleToGrpAggState(a *Aggregator, t *Tuple, grpAggState *[]AggState) {
	for i := range *grpAggState {
		if (*grpAggState)[i] == nil {
			(*grpAggState)[i] = a.newAggState[i].Copy()
		}
		(*grpAggState)[i].AddTuple(t)
	}
}

// Given that all child tuples have been added, return an iterator that iterates
// through the finalized aggregate result one group at a time. The returned tuples should
// be structured according to the TupleDesc returned from the Descriptor() method.
// HINT: you can call [aggState.Finalize()] to get the field for each AggState.
// Then, you should get the groupByTuple and merge it with each of the AggState tuples using the
// joinTuples function in tuple.go you wrote in lab 1.
func getFinalizedTuplesIterator(a *Aggregator, groupByList []*Tuple, aggState map[any]*[]AggState) func() (*Tuple, error) {
	curGbyTupleIndex := 0
	return func() (*Tuple, error) {
		if curGbyTupleIndex < len(groupByList) {
			curGbyTuple := groupByList[curGbyTupleIndex]
			groupAggState := aggState[curGbyTuple.tupleKey()]
			for _, aggState := range *groupAggState {
				curGbyTuple = joinTuples(curGbyTuple, aggState.Finalize())
			}
			curGbyTupleIndex++
			return curGbyTuple, nil
		}
		return nil, nil
	}
}
