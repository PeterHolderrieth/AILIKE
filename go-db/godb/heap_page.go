package godb

import (
	"bytes"
	"encoding/binary"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapRecordId struct {
	fileName string
	pageNo   int
	slotNo   int
}

type heapPage struct {
	numSlots     int32
	numOpenSlots int32
	pageNo       int
	filePointer  *HeapFile
	records      []*Tuple
	dirty        bool
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	numSlots, err := desc.getNumSlotsPerPage(PageSize)
	if err != nil {
		panic(err.Error())
	}
	records := make([]*Tuple, numSlots)

	return &heapPage{numSlots: int32(numSlots), numOpenSlots: numSlots, pageNo: pageNo, filePointer: f, records: records}
}

func (h *heapPage) getNumOpenSlots() int {
	return int(h.numOpenSlots)
}

func (h *heapPage) getNumSlots() int {
	return int(h.numSlots)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	if h.numOpenSlots == 0 {
		return nil, GoDBError{PageFullError, "No empty slots in heap page."}
	}
	for i, r := range h.records {
		if r == nil {
			h.records[i] = t
			h.numOpenSlots -= 1
			h.setDirty(true)
			return heapRecordId{pageNo: h.pageNo, slotNo: i, fileName: h.filePointer.fileName}, nil
		}
	}
	// If we get here we are in a bad state... not sure what to do about that.
	// TODO(tally): handle this in a more appropriate way
	return nil, GoDBError{PageFullError, "No empty slots in heap page; page is malformed."}
}

// Returns the tuple with the specified rid, or return an error if tuple not found
func (h *heapPage) findTuple(rid recordID) (*Tuple, error) {
	if rid.(heapRecordId).pageNo != h.pageNo {
		panic("Trying to find record from wrong page.")
	}
	slotNo := rid.(heapRecordId).slotNo
	if h.records[slotNo] != nil {
		return h.records[slotNo], nil
	}
	return nil, GoDBError{IllegalOperationError, "Trying to find a non-existant tuple."}
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	if rid.(heapRecordId).pageNo != h.pageNo {
		panic("Trying to delete record from wrong page.")
	}
	slotNo := rid.(heapRecordId).slotNo
	if h.records[slotNo] != nil {
		h.records[slotNo] = nil
		h.numOpenSlots += 1
		h.setDirty(true)
		return nil
	}
	return GoDBError{IllegalOperationError, "Trying to delete a non-existant tuple."}
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirty //replace me
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	var f DBFile = p.filePointer
	return &f
}

func (p *heapPage) flushPage() error {
	var page Page = p
	return p.filePointer.flushPage(&page)
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	b := new(bytes.Buffer)
	err := binary.Write(b, binary.LittleEndian, h.numSlots)
	if err != nil {
		return nil, err
	}
	err = binary.Write(b, binary.LittleEndian, h.numOpenSlots)
	if err != nil {
		return nil, err
	}
	for _, r := range h.records {
		if r != nil {
			if err := r.writeTo(b); err != nil {
				return nil, err
			}
		}
	}
	return b, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var numSlots int32
	if err := binary.Read(buf, binary.LittleEndian, &numSlots); err != nil {
		return err
	}
	records := make([]*Tuple, numSlots)

	var numOpenSlots int32
	if err := binary.Read(buf, binary.LittleEndian, &numOpenSlots); err != nil {
		return err
	}

	fileName := (*h.getFile()).(*HeapFile).fileName
	for i := 0; i < int(numSlots-numOpenSlots); i++ {
		t, err := readTupleFrom(buf, h.filePointer.Descriptor())
		if err != nil {
			return err
		}
		t.Rid = heapRecordId{
			pageNo:   h.pageNo,
			slotNo:   i,
			fileName: fileName,
		}
		records[i] = t
	}

	h.numSlots = numSlots
	h.numOpenSlots = numOpenSlots
	h.records = records
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	var slotNo int = 0
	return func() (*Tuple, error) {
		for slotNo < int(p.numSlots) && p.records[slotNo] == nil {
			slotNo += 1
		}
		if slotNo == int(p.numSlots) {
			return nil, nil
		}
		t := p.records[slotNo]
		slotNo += 1
		return t, nil
	}
}
