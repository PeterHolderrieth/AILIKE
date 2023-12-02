package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	UnknownType        DBType = iota //used internally, during parsing, because sometimes the type is unknown
	IntType            DBType = iota
	StringType         DBType = iota
	EmbeddedStringType DBType = iota
	VectorFieldType  DBType = iota
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string", EmbeddedStringType: "text", VectorFieldType: "vec"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// Compare two FieldTypes, and return true iff the Ftypes and Fnames are equal
func (ft1 *FieldType) equals(ft2 *FieldType) bool {
	return ft1.Fname == ft2.Fname && ft1.Ftype == ft2.Ftype
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}
	for i := 0; i < len(d1.Fields); i++ {
		if !d1.Fields[i].equals(&d2.Fields[i]) {
			return false
		}
	}
	return true
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(f FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, fi := range desc.Fields {
		if fi.Fname == f.Fname && (fi.Ftype == f.Ftype || f.Ftype == UnknownType) {
			if f.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", fi.Fname)}
			}
			if fi.TableQualifier == f.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", f.TableQualifier, f.Fname)}
}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	fieldsCopy := make([]FieldType, len(td.Fields))
	copy(fieldsCopy, td.Fields)
	return &TupleDesc{Fields: fieldsCopy}
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	mergedFields := make([]FieldType, len(desc.Fields)+len(desc2.Fields))
	copy(mergedFields, desc.Fields)
	copy(mergedFields[len(desc.Fields):], desc2.Fields)
	return &TupleDesc{Fields: mergedFields}
}

// Gives the byte size of a Tuple with the given TupleDesc desc
func (desc *TupleDesc) sizeInBytes() int {
	var numVec int = 0
	var numTexts int = 0
	var numStrings int = 0
	var numInts int = 0
	for _, f := range desc.Fields {
		switch f.Ftype {
		case IntType:
			numInts += 1
		case StringType:
			numStrings += 1
		case EmbeddedStringType:
			numTexts += 1
		case VectorFieldType:
			numVec += 1
		default:
			panic("Cannot get size in bytes for unknown field type.")
		}
	}
	return StringLength*numStrings + IntSizeBytes*numInts + numTexts*TextSizeBytes + numVec*EmbeddingSizeBytes
}

// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// String field value
type EmbeddedStringField struct {
	Value string
	Emb   EmbeddingType
}

// String field value
type VectorField struct {
	Emb   EmbeddingType
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface{}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian order.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	for i, f := range t.Fields {

		switch f := f.(type) {
		case StringField:
			if t.Desc.Fields[i].Ftype != StringType {
				return GoDBError{TypeMismatchError, "Tuple's fields do not match its descriptor."}
			}
			stringBytes := make([]byte, StringLength)
			copy(stringBytes, []byte(f.Value))
			err := binary.Write(b, binary.LittleEndian, stringBytes)
			if err != nil {
				return err
			}

		case IntField:
			if t.Desc.Fields[i].Ftype != IntType {
				return GoDBError{TypeMismatchError, "Tuple's fields do not match its descriptor."}
			}
			err := binary.Write(b, binary.LittleEndian, &f.Value)
			if err != nil {
				return err
			}

		case EmbeddedStringField:

			if t.Desc.Fields[i].Ftype != EmbeddedStringType {
				return GoDBError{TypeMismatchError, "Tuple's fields do not match its descriptor."}
			}

			//Add embedding
			for _, embValue := range f.Emb {
				err := binary.Write(b, binary.LittleEndian, embValue)
				if err != nil {
					return err
				}
			}
			//Add text
			TextBytes := make([]byte, TextCharLength)
			copy(TextBytes, []byte(f.Value))
			err := binary.Write(b, binary.LittleEndian, TextBytes)
			if err != nil {
				return err
			}
		case VectorField:
			if t.Desc.Fields[i].Ftype != VectorFieldType {
				return GoDBError{TypeMismatchError, "Tuple's fields do not match its descriptor."}
			}

			//Add embedding
			for _, embValue := range f.Emb {
				err := binary.Write(b, binary.LittleEndian, embValue)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.

func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	var stringBytes = make([]byte, StringLength)
	var textBytes = make([]byte, TextCharLength)
	var nextString string
	var nextInt int64
	var nextFloat float64
	var nextText string

	tupleFields := make([]DBValue, len(desc.Fields))
	for i, f := range desc.Fields {
		switch f.Ftype {
		case StringType:
			err := binary.Read(b, binary.LittleEndian, &stringBytes)
			if err != nil {
				return nil, err
			}
			nextString = string(bytes.TrimRight(stringBytes, "\x00"))
			tupleFields[i] = StringField{nextString}
		case IntType:
			err := binary.Read(b, binary.LittleEndian, &nextInt)
			if err != nil {
				return nil, err
			}
			tupleFields[i] = IntField{nextInt}

		case EmbeddedStringType:

			//Read embedding
			var emb EmbeddingType
			for i := 0; i < TextEmbeddingDim; i++ {
				err := binary.Read(b, binary.LittleEndian, &nextFloat)
				if err != nil {
					return nil, err
				}
				emb = append(emb, nextFloat)
			}

			// Read
			err := binary.Read(b, binary.LittleEndian, &textBytes)
			if err != nil {
				return nil, err
			}
			nextText = string(bytes.TrimRight(textBytes, "\x00"))
			tupleFields[i] = EmbeddedStringField{Value: nextText, Emb: emb}
		case VectorFieldType:
			//Read embedding
			var emb EmbeddingType
			for i := 0; i < TextEmbeddingDim; i++ {
				err := binary.Read(b, binary.LittleEndian, &nextFloat)
				if err != nil {
					return nil, err
				}
				emb = append(emb, nextFloat)
			}
			tupleFields[i] = VectorField{Emb: emb}
		}
	}
	return &Tuple{Desc: *desc, Fields: tupleFields}, nil
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {

	if !t1.Desc.equals(&t2.Desc) {
		return false
	}

	for i, tdesc := range t1.Desc.Fields {
		switch tdesc.Ftype {
		case StringType:
			if t1.Fields[i].(StringField).Value != t2.Fields[i].(StringField).Value {
				return false
			}

		case IntType:
			if t1.Fields[i].(IntField).Value != t2.Fields[i].(IntField).Value {
				return false
			}
		//we assume embeddings will have equal value so we only check the text/value field
		case EmbeddedStringType:
			if t1.Fields[i].(EmbeddedStringField).Value != t2.Fields[i].(EmbeddedStringField).Value {
				return false
			}
		
		case VectorFieldType:
			emb1 := t1.Fields[i].(VectorField).Emb
			emb2 := t2.Fields[i].(VectorField).Emb
			if !equal(&emb1, &emb2) {
				return false
			}
		}
	}

	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	if t1 == nil {
		return t2
	}
	if t2 == nil {
		return t1
	}
	mergedFields := make([]DBValue, len(t1.Fields)+len(t2.Fields))
	copy(mergedFields, t1.Fields)
	copy(mergedFields[len(t1.Fields):], t2.Fields)
	return &Tuple{Desc: *t1.Desc.merge(&t2.Desc), Fields: mergedFields}
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	e1, err := field.EvalExpr(t)
	if err != nil {
		return OrderedEqual, err
	}
	e2, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, err
	}

	switch field.GetExprType().Ftype {
	case StringType:
		v1 := e1.(StringField).Value
		v2 := e2.(StringField).Value
		if v1 < v2 {
			return OrderedLessThan, nil
		} else if v1 == v2 {
			return OrderedEqual, nil
		}
		return OrderedGreaterThan, nil
	case IntType:
		v1 := e1.(IntField).Value
		v2 := e2.(IntField).Value
		if v1 < v2 {
			return OrderedLessThan, nil
		} else if v1 == v2 {
			return OrderedEqual, nil
		}
		return OrderedGreaterThan, nil

	case EmbeddedStringType:
		v1 := e1.(EmbeddedStringField).Value
		v2 := e2.(EmbeddedStringField).Value
		if v1 < v2 {
			return OrderedLessThan, nil
		} else if v1 == v2 {
			return OrderedEqual, nil
		}
		return OrderedGreaterThan, nil
	case VectorFieldType:
		v1 := e1.(VectorField).Emb
		v2 := e2.(VectorField).Emb

		// Compare using magnitude
		v1_dist := float64(0);
		for _, val := range v1{
			v1_dist += val * val;
		}

		v2_dist := float64(0);
		for _, val := range v2{
			v2_dist += val * val;
		}

		if v1_dist < v2_dist {
			return OrderedLessThan, nil
		} else if v1_dist == v2_dist {
			return OrderedEqual, nil
		}
		return OrderedGreaterThan, nil
	}

	return OrderedEqual, GoDBError{IllegalOperationError, "Cannot compare expressions with unknown type."}
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	descWithoutTableQualifiers := *t.Desc.copy()
	for i, f := range descWithoutTableQualifiers.Fields {
		f.TableQualifier = ""
		descWithoutTableQualifiers.Fields[i] = f
	}

	newFields := make([]DBValue, len(fields))
	for i, ft := range fields {
		tfi, err := findFieldInTd(ft, &t.Desc)
		if err != nil {
			if err.(GoDBError).code == IncompatibleTypesError {
				tfi, err = findFieldInTd(ft, &descWithoutTableQualifiers)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
		newFields[i] = t.Fields[tfi]
	}

	descFieldsCopy := make([]FieldType, len(fields))
	copy(descFieldsCopy, fields)
	newTupleDesc := &TupleDesc{Fields: descFieldsCopy}

	return &Tuple{Desc: *newTupleDesc, Fields: newFields, Rid: 0}, nil
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {

	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)

	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		case EmbeddedStringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}
