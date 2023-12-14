package godb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	desc     TupleDesc
	fileName string
	bufPool  *BufferPool
	// pageFull is used to memoize which pages are full by mapping pageNos
	// to whether or not that page is full. The value for pageFull[i] will
	// default to false until the first time page i is read.
	pageFull *sync.Map
	// maps column names to indexes that exist for that column; we currently assume at most one index per column
	indexes map[string]*NNIndexFile
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	indexes := make(map[string]*NNIndexFile) // TODO(tally): populate indexes correctly
	return NewHeapFileIndex(fromFile, td, bp, indexes)
}

func NewHeapFileIndex(fromFile string, td *TupleDesc, bp *BufferPool, indexes map[string]*NNIndexFile) (*HeapFile, error) {
	var pageFull sync.Map
	_, err := os.Stat(fromFile)
	if os.IsNotExist(err) {
		file, err := os.Create(fromFile)
		if err != nil {
			return nil, ailikeError{OSError, err.Error()}
		}
		file.Close()
	} else if err != nil {
		return nil, ailikeError{OSError, err.Error()}
	}
	return &HeapFile{fileName: fromFile, desc: *td.copy(), bufPool: bp, pageFull: &pageFull, indexes: indexes}, nil
}

// Return the number of bytes in file
func (f *HeapFile) FileByteSize() int {

	fInfo, err := os.Stat(f.fileName)
	if err != nil {
		log.Fatal(err)
	}
	return int(fInfo.Size())
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {

	fileSize := f.FileByteSize()
	if fileSize == 0 {
		return 0
	}
	return 1 + int((fileSize-1))/int(PageSize)
}

// Return the number of total tuples in file assuming full pages
func (f *HeapFile) ApproximateNumTuples() int {
	nTPerPage, err := f.desc.getNumSlotsPerPage(PageSize)
	if err != nil {
		panic(err.Error())
	}
	n_tuples_per_page := int(nTPerPage)
	return n_tuples_per_page * f.NumPages()
}

// Return the number of tuples in the heap file
func (f *HeapFile) NumTuples(tid TransactionID) int {
	var numTuples int = 0
	for pageNo := 0; pageNo < f.NumPages(); pageNo++ {
		hp, err := f.getHeapPage(pageNo, tid, ReadPerm)
		if err != nil {
			return -1
		}
		pageTuples := int(hp.numSlots - hp.numOpenSlots)
		numTuples += pageTuples
		// fmt.Println("Page: ", pageNo, "Tuples: ", pageTuples)
	}

	return numTuples
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	desc := f.Descriptor()
	if desc == nil || desc.Fields == nil {
		return ailikeError{MalformedDataError, "Descriptor was nil"}
	}
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.SplitN(line, sep, len(desc.Fields))
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		if numFields != len(desc.Fields) {
			return ailikeError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return ailikeError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			case EmbeddedStringType:
				if len(field) > TextCharLength {
					field = field[0:TextCharLength]
				}
				newFields = append(newFields, EmbeddedStringField{Value: field})
			default:
				return ailikeError{code: IncompatibleTypesError, errString: "(LoadFromCSV): Unknown type."}
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	if pageNo >= f.NumPages() {
		return nil, ailikeError{IllegalOperationError, "Cannot read non-existant page."}
	}

	pageBytes := make([]byte, PageSize)
	filePointer, err := os.OpenFile(f.fileName, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	n, err := filePointer.ReadAt(pageBytes, int64(pageNo*PageSize))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	err = filePointer.Close()
	if err != nil {
		return nil, err
	}
	hp := newHeapPage(f.Descriptor(), pageNo, f)
	buf := bytes.NewBuffer(pageBytes[0:n])
	if err := hp.initFromBuffer(buf); err != nil {
		return nil, err
	}
	f.pageFull.Store(pageNo, hp.numOpenSlots == 0)
	var p Page = hp
	return &p, nil
}

func (f *HeapFile) getHeapPage(pageNo int, tid TransactionID, perm RWPerm) (*heapPage, error) {
	p, err := f.bufPool.GetPage(f, pageNo, tid, perm)
	if err != nil {
		return nil, err
	}
	hp := (*p).(*heapPage)
	return hp, nil
}

// GetPageForInsert finds a page with an available slot for inserting
// If all pages are full, returns nil pointer.
func (f *HeapFile) getPageForInsert(tid TransactionID) (*heapPage, error) {
	for {
		// Iterate over all pages and check if the cached pages have open slots.
		for pageNo := f.NumPages(); pageNo >= 0; pageNo-- {
			if isFull, loaded := f.pageFull.Load(pageNo); loaded && isFull.(bool) {
				continue
			}
			if f.bufPool.hasPageCached(f, pageNo, tid, WritePerm) {
				hp, err := f.getHeapPage(pageNo, tid, WritePerm)
				if err == nil {
					if hp.numOpenSlots > 0 {
						return hp, nil
					}
				}
			}
		}

		// If there is no cached page with an open slot, look for an existing page with an open slot
		for pageNo := f.NumPages(); pageNo >= 0; pageNo-- {
			if isFull, loaded := f.pageFull.Load(pageNo); loaded && isFull.(bool) {
				continue
			}
			hp, err := f.getHeapPage(pageNo, tid, WritePerm)
			if err == nil {
				if hp.numOpenSlots > 0 {
					return hp, nil
				}
			}
		}

		newPageNo := f.NumPages()
		// It is possible a different thread has already created this page, so we try to get it.
		// This also gets an exclusive lock on the new page number, preventing another transaction from creating the page.
		hp, err := f.getHeapPage(newPageNo, tid, WritePerm)
		if err == nil {
			if hp.numOpenSlots > 0 {
				return hp, nil
			}
			// page was already full, so we try to get a different page by retrying from beginning
		} else {
			// The new page didn't exist, so we create a new page now.
			np := newHeapPage(f.Descriptor(), newPageNo, f)
			err = np.flushPage() // flush the empty page to disk to update the page count
			if err != nil {
				return nil, err
			}
			// get new page from bufPool
			hp, err := f.getHeapPage(newPageNo, tid, WritePerm)
			if err != nil {
				return nil, err
			}
			return hp, nil
		}
	}
}

func (f *HeapFile) _insertTupleHelper(hp *heapPage, t *Tuple, tid TransactionID) error {
	rid, err := hp.insertTuple(t)
	if err != nil {
		return err
	}
	t.Rid = rid
	f.pageFull.Store(hp.pageNo, hp.numOpenSlots == 0)

	// Insert tuple into all associated secondary indexes
	for _, index := range f.indexes {
		if index.clustered {
			continue
		}
		err = index.insertTuple(t, tid)
		if err != nil {
			return err
		}
	}
	return nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	// Create embedding for every embedded string field; note we do not call
	// this method in insertTupleIntoPage or insertTupleIntoNewPage because
	// those methods are only called once the embedding has already been
	// generated; TODO: consider refactoring this to be more explicit about this behavior.
	for i, field := range t.Desc.Fields {
		if field.Ftype == EmbeddedStringType {
			EmbeddedStringField := t.Fields[i].(EmbeddedStringField)
			embResp, err := generateEmbeddings(EmbeddedStringField.Value)
			if err != nil {
				return err
			}
			EmbeddedStringField.Emb = embResp.Embedding
			t.Fields[i] = EmbeddedStringField
		}
	}

	var clusteredIndex *NNIndexFile = nil
	for _, index := range f.indexes {
		if index.clustered {
			if clusteredIndex != nil {
				return ailikeError{IncompatibleTypesError, "(insertTuple): Multiple clustered indexes found."}
			}
			clusteredIndex = index
		}
	}
	if clusteredIndex != nil {
		return clusteredIndex.insertTuple(t, tid)
	}
	hp, err := f.getPageForInsert(tid)
	if err != nil {
		return err
	}
	return f._insertTupleHelper(hp, t, tid)
}

// Add the tuple to the HeapFile to a specific page. If that page is full,
// returns an error.
func (f *HeapFile) insertTupleIntoPage(t *Tuple, pageNo int, tid TransactionID) error {
	hp, err := f.getHeapPage(pageNo, tid, WritePerm)
	if err != nil {
		return err
	}
	if hp.numOpenSlots <= 0 {
		return ailikeError{PageFullError, "Cannot insert into full page."}
	}
	return f._insertTupleHelper(hp, t, tid)
}

// Makes a new heap page and returns it's page number, a pointer to the page, and an error.
func (f *HeapFile) makeNewPage(tid TransactionID) (*heapPage, int, error) {
	var np *heapPage = nil
	var newPageNo int = -1
	for np == nil {
		newPageNo = f.NumPages()

		// It is possible a different thread has already created this page, so we try to get it.
		// This also gets an exclusive lock on the new page number, preventing another transaction from creating the page.
		// If this does not fail; that means another transaction already created the page, so we need
		// to try and create another one.
		_, err := f.getHeapPage(newPageNo, tid, WritePerm)
		if err == nil {
			continue
		} else {
			// The new page didn't exist, so we create a new page now.
			hp := newHeapPage(f.Descriptor(), newPageNo, f)
			err = hp.flushPage() // flush the empty page to disk to update the page count
			if err != nil {
				return nil, -1, err
			}
			// get new page from bufPool
			np, err = f.getHeapPage(newPageNo, tid, WritePerm)
			if err != nil {
				return nil, -1, err
			}
		}
	}
	return np, newPageNo, nil
}

// Add the tuple to the HeapFile to a new page. Returns the page number of the
// new page.
func (f *HeapFile) insertTupleIntoNewPage(t *Tuple, tid TransactionID) (int, error) {
	np, newPageNo, err := f.makeNewPage(tid)
	if err != nil {
		return -1, err
	}
	if np.numOpenSlots <= 0 {
		return -1, ailikeError{PageFullError, "Cannot insert into full page."}
	}
	err = f._insertTupleHelper(np, t, tid)
	if err != nil {
		return -1, err
	}
	return newPageNo, nil
}

// Finds the tuple with the given rid and returns it.
func (f *HeapFile) findTuple(rid heapRecordId, tid TransactionID) (*Tuple, error) {
	if rid.fileName != f.fileName {
		return nil, ailikeError{TupleNotFoundError, "Tuple does not exist within this file."}
	}
	hp, err := f.getHeapPage(rid.pageNo, tid, WritePerm)
	if err != nil {
		return nil, err
	}
	return hp.findTuple(rid)
}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	rid := t.Rid.(heapRecordId)
	if rid.fileName != f.fileName {
		return ailikeError{TupleNotFoundError, "Tuple does not exist within this file."}
	}
	hp, err := f.getHeapPage(rid.pageNo, tid, WritePerm)
	if err != nil {
		return err
	}
	err = hp.deleteTuple(rid)
	if err != nil {
		return err
	}
	f.pageFull.Store(hp.pageNo, false)

	for _, index := range f.indexes {
		err = index.deleteTuple(t, tid)
		if err != nil {
			return err
		}
	}
	return nil
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	var hp *heapPage = (*p).(*heapPage)
	pageBuf, err := hp.toBuffer()
	if err != nil {
		return err
	}
	b := pageBuf.Bytes()
	filePointer, err := os.OpenFile(f.fileName, os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	_, err = filePointer.WriteAt(b, int64(hp.pageNo*PageSize))
	if err != nil {
		return err
	}
	err = filePointer.Close()
	if err != nil {
		return err
	}
	hp.setDirty(false)
	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return &f.desc
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	var pageNo int = 0
	var tupleIter func() (*Tuple, error) = func() (*Tuple, error) {
		return nil, nil
	}
	return func() (*Tuple, error) {
		var t *Tuple
		t, err := tupleIter()
		if err != nil {
			return nil, err
		}
		for t == nil && pageNo < f.NumPages() {
			// Try to get the tuple iter for the next page and return that tuple
			hp, err := f.getHeapPage(pageNo, tid, ReadPerm)
			pageNo += 1
			if err != nil {
				return nil, err
			}
			tupleIter = hp.tupleIter()
			t, err = tupleIter()
			if err != nil {
				return nil, err
			}
		}
		return t, nil

	}, nil

}

// internal strucuture to use as key for a heap page
type HeapFilePageKey struct {
	fileName string
	pageNo   int
}

func (hpk HeapFilePageKey) getFileName() string {
	return hpk.fileName
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// HeapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) BufferPoolKey {
	return HeapFilePageKey{fileName: f.fileName, pageNo: pgNo}
}
