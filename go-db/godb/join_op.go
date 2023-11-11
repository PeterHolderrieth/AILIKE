package godb

type EqualityJoin[T comparable] struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator //operators for the two inputs of the join

	// Function that when applied to a DBValue returns the join value; will be
	// one of intFilterGetter or stringFilterGetter
	getter func(DBValue) T

	// The maximum number of records of intermediate state that the join should use
	// (only required for optional exercise)
	maxBufferSize int
}

// Constructor for a  join of integer expressions
// Returns an error if either the left or right expression is not an integer
func NewIntJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[int64], error) {
	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	if left == nil || right == nil {
		return nil, GoDBError{MalformedDataError, "NewIntJoin left or right pointer is nil."}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return nil, GoDBError{TypeMismatchError, "join field is not an int"}
	case IntType:
		return &EqualityJoin[int64]{leftField, rightField, &left, &right, intFilterGetter, maxBufferSize}, nil
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Constructor for a  join of string expressions
// Returns an error if either the left or right expression is not a string
func NewStringJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[string], error) {
	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	if left == nil || right == nil {
		return nil, GoDBError{MalformedDataError, "NewStringJoin left or right pointer is nil."}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return &EqualityJoin[string]{leftField, rightField, &left, &right, stringFilterGetter, maxBufferSize}, nil
	case IntType:
		return nil, GoDBError{TypeMismatchError, "join field is not a string"}
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Return a TupleDescriptor for this join. The returned descriptor should contain
// the union of the fields in the descriptors of the left and right operators.
// HINT: use the merge function you implemented for TupleDesc in lab1
// TODO: what happens with duplicate field names?
func (hj *EqualityJoin[T]) Descriptor() *TupleDesc {
	return (*hj.left).Descriptor().merge((*hj.right).Descriptor())
}

func (joinOp *EqualityJoin[T]) buildBlockHashMap(leftIter func() (*Tuple, error)) (map[T][]*Tuple, error) {
	blockHashMap := make(map[T][]*Tuple)
	for i := 0; i < joinOp.maxBufferSize; i++ {
		curLeftT, err := leftIter()
		if err != nil {
			return nil, err
		}
		if curLeftT == nil {
			break
		}
		leftV, err := joinOp.leftField.EvalExpr(curLeftT)
		if err != nil {
			return nil, err
		}
		leftFieldVal := joinOp.getter(leftV)
		bucket, ok := blockHashMap[leftFieldVal]
		if !ok {
			bucket = make([]*Tuple, 0, 10)
		}
		bucket = append(bucket, curLeftT)
		blockHashMap[leftFieldVal] = bucket
	}
	return blockHashMap, nil
}

// Join operator implementation.  This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
// HINT: When implementing the simple nested loop join, you should keep in mind that
// you only iterate through the left iterator once (outer loop) but iterate through the right iterator
// once for every tuple in the the left iterator (inner loop).
// HINT: You can use joinTuples function you implemented in lab1 to join two tuples.
//
// OPTIONAL EXERCISE:  the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out.  To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	leftIter, err := (*joinOp.left).Iterator(tid)
	if err != nil {
		return nil, err
	}
	if leftIter == nil {
		return nil, GoDBError{MalformedDataError, "EqualityJoin left Iterator unexpectedly nil."}
	}

	rightIter, err := (*joinOp.right).Iterator(tid)
	if err != nil {
		return nil, err
	}
	if rightIter == nil {
		return nil, GoDBError{MalformedDataError, "EqualityJoin right Iterator unexpectedly nil."}
	}

	blockHashMap, err := joinOp.buildBlockHashMap(leftIter)
	if err != nil {
		return nil, err
	}
	curRightT, err := rightIter()
	if err != nil {
		return nil, err
	}

	var (
		curBucketVal   T
		curBucket      []*Tuple
		curBucketIndex int = 0
		curLeftT       *Tuple
		ok             bool
	)

	return func() (*Tuple, error) {
		for len(blockHashMap) > 0 {
			for curRightT != nil {
				// Compute the rightFieldVal for the curren right tuple
				rightV, err := joinOp.rightField.EvalExpr(curRightT)
				if err != nil {
					return nil, err
				}
				rightFieldVal := joinOp.getter(rightV)

				if rightFieldVal == curBucketVal && curBucketIndex < len(curBucket) {
					// If rightFieldVal matches the value for the current bucket we are iterating over,
					// and there are remaining tuples in the bucket, get the next left-hand tuple from
					// that bucket and return the joined tuple.
					curLeftT = curBucket[curBucketIndex]
					curBucketIndex++
					return joinTuples(curLeftT, curRightT), nil
				} else if rightFieldVal == curBucketVal && curBucketIndex == len(curBucket) && len(curBucket) > 0 {
					// If we have reached the end of the bucket, we need to get the next right-hand tuple.
					curBucketIndex = 0
					curRightT, err = rightIter()
					if err != nil {
						return nil, err
					}
				} else {
					// If current rightFieldVal does not currently match bucket value, we check if
					// the rightFieldValue is in the hash map and fetch the matching bucket if
					// present. Otherwise, we know there are no matching tuples so we get the next
					// right-hand tuple. Note: When we find a matching bucket, a tuple will not get
					// returned until the next iteration through this for loop.
					curBucket, ok = blockHashMap[rightFieldVal]
					if ok {
						curBucketVal = rightFieldVal
					} else {
						curRightT, err = rightIter()
						if err != nil {
							return nil, err
						}
					}
				}
			}

			// When we have finished iterating through the right-hand iterator for this block,
			// we build the next block's hash map...
			blockHashMap, err = joinOp.buildBlockHashMap(leftIter)
			if err != nil {
				return nil, err
			}
			// and reset the right-hand iterator.
			rightIter, err = (*joinOp.right).Iterator(tid)
			if err != nil {
				return nil, err
			}
			curRightT, err = rightIter()
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}, nil
}
