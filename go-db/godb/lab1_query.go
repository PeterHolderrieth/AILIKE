package godb

import "os"

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {

	fieldIndex, err := findFieldInTd(FieldType{Fname: sumField, Ftype: IntType}, &td)
	if err != nil {
		return -1, err
	}

	TEMP_FILE_NAME := "computeFieldSum.temp.dat"
	bp := NewBufferPool(10)
	hf, err := NewHeapFile(TEMP_FILE_NAME, &td, bp)
	if err != nil {
		return -1, err
	}
	csvf, err := os.Open(fileName)
	if err != nil {
		return -1, err
	}
	// Assuming the csvs will always have headers and be comma-delimited
	err = hf.LoadFromCSV(csvf, true, ",", false)
	if err != nil {
		return -1, err
	}

	tid := NewTID()
	nextTuple, err := hf.Iterator(tid)
	if err != nil {
		return -1, err
	}

	var v int64
	var sum int64 = 0
	for t, err := nextTuple(); err == nil && t != nil; t, err = nextTuple() {
		v = t.Fields[fieldIndex].(IntField).Value
		sum += v
	}

	err = os.Remove(TEMP_FILE_NAME)
	if err != nil {
		return -1, err
	}
	return int(sum), nil // replace me
}
