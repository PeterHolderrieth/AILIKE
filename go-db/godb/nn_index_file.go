package godb

// TupleDesc for the heap file that stores the maping from vectors to heapRecordIds.
// heapRecordIds are composed from filenames, pageNo, slotNo; we already know
// the table's filename, so we don't need to store it in the dataHeapFile, though we could.
var dataDesc = TupleDesc{Fields: []FieldType{
	{Fname: "vector", Ftype: VectorFieldType},
	{Fname: "tablePageNo", Ftype: IntType},
	{Fname: "slotNo", Ftype: IntType},
}}

// TupleDesc for the heap file that stores the maping from centroids to index pageNos.
var centroidDesc = TupleDesc{Fields: []FieldType{
	{Fname: "vector", Ftype: VectorFieldType},
	{Fname: "centroidId", Ftype: IntType},
}}

// TupleDesc for the heap file that stores the maping from centroids to index pageNos.
var mappingDesc = TupleDesc{Fields: []FieldType{
	{Fname: "centroidId", Ftype: IntType},
	{Fname: "indexPageNo", Ftype: IntType},
}}

// NNIndexFile provides a nearest-neighbor index for a given table stored within a HeapFile.
type NNIndexFile struct {
	tableFileName  string // the filename of the table this is an index for
	indexedColName string // the name of the column being indexed; must be EmbeddedString column
	// We use a heapFile to store the vector <-> heapRecordId mappings
	dataHeapFile *HeapFile
	// We use another heap file to store centroid <-> centoidId mappings
	centroidHeapFile *HeapFile
	// We use a third heap file to store centroidId <-> pageNo, where pageNo is a page that contains 
	// rows for the given centroid within the dataHeapFile 
	mappingHeapFile *HeapFile 
}

// Create a NnIndexFile.
// Parameters
// - fromTableFile: the filename for the HeapFile for the Table that this NN index is for.
// - indexedColName: the column in the table that is indexed
// - fromDataFile: the backing file for this index that store the vector <-> heapRecordId mapping
// - fromCentroidFile: the backing file for this index that stores the centroid <-> pageNo mapping
// - bp: the BufferPool that is used to store pages read from this index
// May return an error if the file cannot be opened or created.
func NewNNIndexFileFile(tableFileName string, indexedColName string, fromDataFile string, fromCentroidFile string, fromMappingFile string, bp *BufferPool) (*NNIndexFile, error) {
	dataHeapFile, err := NewHeapFile(fromDataFile, &dataDesc, bp)
	if err != nil {
		return nil, err
	}
	centroidHeapFile, err := NewHeapFile(fromCentroidFile, &centroidDesc, bp)
	if err != nil {
		return nil, err
	}
	mappingHeapFile, err := NewHeapFile(fromMappingFile, &mappingDesc, bp)
	if err != nil {
		return nil, err
	}
	return &NNIndexFile{tableFileName, indexedColName, dataHeapFile, centroidHeapFile, mappingHeapFile}, nil
}
