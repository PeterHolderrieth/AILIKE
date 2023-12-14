package godb

import (
	"fmt"
	"regexp"
	"strings"
	"unsafe"

	"golang.org/x/exp/constraints"
)

type ailikeErrorCode int

const (
	TupleNotFoundError      ailikeErrorCode = iota
	PageFullError           ailikeErrorCode = iota
	IncompatibleTypesError  ailikeErrorCode = iota
	TypeMismatchError       ailikeErrorCode = iota
	MalformedDataError      ailikeErrorCode = iota
	BufferPoolFullError     ailikeErrorCode = iota
	ParseError              ailikeErrorCode = iota
	DuplicateTableError     ailikeErrorCode = iota
	NoSuchTableError        ailikeErrorCode = iota
	AmbiguousNameError      ailikeErrorCode = iota
	IllegalOperationError   ailikeErrorCode = iota
	DeadlockError           ailikeErrorCode = iota
	IllegalTransactionError ailikeErrorCode = iota
	FailedEmbedding         ailikeErrorCode = iota
	UnknownClusterError     ailikeErrorCode = iota
	OSError                 ailikeErrorCode = iota
)

type ailikeError struct {
	code      ailikeErrorCode
	errString string
}

func (e ailikeError) Error() string {
	return fmt.Sprintf("code %d;  err: %s", e.code, e.errString)
}

const (
	StringLength   int     = 32
	TextCharLength int     = 120
	FloatSizeBytes int     = int(unsafe.Sizeof(float64(0.0)))
	IntSizeBytes   int     = int(unsafe.Sizeof(int64(0)))
	MaxIterKMeans  int     = 10
	DeltaThrKMeans float64 = 1.0
	DefaultProbe   int     = 3
)

var (
	// The following are configurable.
	TextEmbeddingDim int = 384
	PageSize         int = 8192

	// the following will change based on configurable variables
	EmbeddingSizeBytes int = TextEmbeddingDim * FloatSizeBytes
	TextSizeBytes      int = EmbeddingSizeBytes + TextCharLength
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
