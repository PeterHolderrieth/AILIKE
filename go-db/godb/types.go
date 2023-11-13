package godb

import (
	"fmt"
	"regexp"
	"strings"
	"unsafe"

	"golang.org/x/exp/constraints"
)

type GoDBErrorCode int

const (
	TupleNotFoundError      GoDBErrorCode = iota
	PageFullError           GoDBErrorCode = iota
	IncompatibleTypesError  GoDBErrorCode = iota
	TypeMismatchError       GoDBErrorCode = iota
	MalformedDataError      GoDBErrorCode = iota
	BufferPoolFullError     GoDBErrorCode = iota
	ParseError              GoDBErrorCode = iota
	DuplicateTableError     GoDBErrorCode = iota
	NoSuchTableError        GoDBErrorCode = iota
	AmbiguousNameError      GoDBErrorCode = iota
	IllegalOperationError   GoDBErrorCode = iota
	DeadlockError           GoDBErrorCode = iota
	IllegalTransactionError GoDBErrorCode = iota
)

type GoDBError struct {
	code      GoDBErrorCode
	errString string
}

func (e GoDBError) Error() string {
	return fmt.Sprintf("code %d;  err: %s", e.code, e.errString)
}

const (
	PageSize           int = 4096
	StringLength       int = 32
	TextEmbeddingDim   int = 768
	TextCharLength     int = 300
	FloatSizeBytes     int = int(unsafe.Sizeof(float64(0.0)))
	EmbeddingSizeBytes int = TextEmbeddingDim * FloatSizeBytes
	TextSizeBytes      int = EmbeddingSizeBytes + TextCharLength
	IntSizeBytes       int = int(unsafe.Sizeof(int64(0)))
)

type Page interface {
	//these methods are used by buffer pool to
	//manage pages
	isDirty() bool
	setDirty(dirty bool)
	getFile() *DBFile
	flushPage() error
	getNumOpenSlots() int
}

type BufferPoolKey interface {
	getFileName() string
}

type DBFile interface {
	insertTuple(t *Tuple, tid TransactionID) error
	deleteTuple(t *Tuple, tid TransactionID) error

	//methods used by buffer pool to manage retrieval of pages
	readPage(pageNo int) (*Page, error)
	flushPage(page *Page) error
	pageKey(pgNo int) BufferPoolKey

	Operator
}

type Operator interface {
	Descriptor() *TupleDesc
	Iterator(tid TransactionID) (func() (*Tuple, error), error)
}

type BoolOp int

const (
	OpGt   BoolOp = iota
	OpLt   BoolOp = iota
	OpGe   BoolOp = iota
	OpLe   BoolOp = iota
	OpEq   BoolOp = iota
	OpNeq  BoolOp = iota
	OpLike BoolOp = iota
)

var BoolOpMap = map[string]BoolOp{
	">":    OpGt,
	"<":    OpLt,
	"<=":   OpLe,
	">=":   OpGe,
	"=":    OpEq,
	"<>":   OpNeq,
	"!=":   OpNeq,
	"like": OpLike,
}

func evalPred[T constraints.Ordered](i1 T, i2 T, op BoolOp) bool {
	switch op {
	case OpEq:
		return i1 == i2
	case OpNeq:
		return i1 != i2
	case OpGt:
		return i1 > i2
	case OpGe:
		return i1 >= i2
	case OpLt:
		return i1 < i2
	case OpLe:
		return i1 <= i2
	case OpLike:
		s1, ok := any(i1).(string)
		if !ok {
			return false
		}
		regex, ok := any(i2).(string)
		if !ok {
			return false
		}
		regex = "^" + regex + "$"
		regex = strings.Replace(regex, "%", ".*?", -1)
		match, _ := regexp.MatchString(regex, s1)
		return match
	}
	return false

}
