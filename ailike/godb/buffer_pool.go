package godb

import (
	"math"
	"sync"
	"time"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by ailike.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used when reading / locking pages
type RWPerm int

// If the ALLOW_RESERVATIONS flag is set to true, then we allow transactions to "reserve" exclusive locks before other
// transactions have released their shared locks. If false, then we wait for transactions to release shared locks.
const ALLOW_RESERVATIONS = true

// If the ABORT_TRANSACTIONS flag is set to true, then the buffer pool will abort transactions when it detects deadlock.
// if false, the buffer pool relies on the calling code to abort transactions.
const ABORT_TRANSACTIONS = true

// My code will pass all the tests under the following configurations:
//   - ALLOW_RESERVATIONS = true, ABORT_TRANSACTIONS = true
//   - ALLOW_RESERVATIONS = false, ABORT_TRANSACTIONS = true
//   - ALLOW_RESERVATIONS = true, ABORT_TRANSACTIONS = false

const BLOCK_TIME = 2 * time.Millisecond // time to wait before attempting to acquire a lock again
const CYCLE_CHECK_INTERVAL = 2          // number of attempts at acquiring a lock before we check for deadlock
const USE_EVICT_QUEUE = false           // if true, use eviction queue to evict pages. Otherwise, evict pages based on number of empty slots.

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type Lock struct {
	pageKey BufferPoolKey
	perm    RWPerm // WritePerm for exclusive locks, ReadPerm for shared locks
}

type BufferPool struct {
	numPages              int
	pageMap               map[BufferPoolKey]Page
	mutex                 *sync.Mutex
	sharedLockMap         map[BufferPoolKey]int           // maps BufferPoolKeys to the number of transactions that are holding a shared lock on the page
	exclusiveLockMap      map[BufferPoolKey]TransactionID // maps BufferPoolKeys to the TransactionID that is holding an exclusive lock
	transactionWaitingFor map[TransactionID]Lock          // maps TransactionIDs to the Lock they are waiting for
	transactionLocks      map[TransactionID]map[Lock]bool // maps TransactionIDs to the Locks they hold or have reserved
	steal                 bool
	evictQueue            []BufferPoolKey
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	pageMap := make(map[BufferPoolKey]Page, numPages)
	var mutex sync.Mutex
	sharedLockMap := make(map[BufferPoolKey]int, 0)
	exclusiveLockMap := make(map[BufferPoolKey]TransactionID, 0)
	transactionWaitingFor := make(map[TransactionID]Lock, 0)
	transactionLocks := make(map[TransactionID]map[Lock]bool, 0)
	evictQueue := make([]BufferPoolKey, 0)
	return &BufferPool{numPages, pageMap, &mutex, sharedLockMap, exclusiveLockMap, transactionWaitingFor, transactionLocks, false, evictQueue}
}

func (bp *BufferPool) EvictPage() error {

	var evictK BufferPoolKey
	var evictP Page = nil
	minOpenSlots := uint(math.Inf(1))

	// Evict page with the least number of empty slots
	for k, page := range bp.pageMap {
		if !page.isDirty() || bp.steal {
			if uint(page.getNumOpenSlots()) <= minOpenSlots {
				minOpenSlots = uint(page.getNumOpenSlots())
				evictK = k
				evictP = page
			}
			if page.getNumOpenSlots() == 0 {
				break // break early if we find a full page.
			}
		}
	}
	if evictP != nil {
		err := evictP.flushPage()
		if err != nil {
			return err
		}
		delete(bp.pageMap, evictK)
		return nil
	}

	return ailikeError{BufferPoolFullError, "Cannot evict page; all pages are dirty."}

}

func (bp *BufferPool) EvictPageQueue() error {
	// Evict page using eviction queue
	for i, evictK := range bp.evictQueue {
		var evictP Page = bp.pageMap[evictK]
		if !evictP.isDirty() || bp.steal {
			err := evictP.flushPage()
			if err != nil {
				return err
			}
			delete(bp.pageMap, evictK)
			bp.evictQueue = append(bp.evictQueue[:i], bp.evictQueue[i+1:]...)
			return nil
		}
	}
	return ailikeError{BufferPoolFullError, "Cannot evict page; all pages are dirty."}
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	for k := range bp.pageMap {
		page := bp.pageMap[k]
		err := page.flushPage()
		if err != nil {
			panic("Could not flush all pages.")
		}
	}
}

func (bp *BufferPool) ClearAllPages() {
	bp.pageMap = make(map[BufferPoolKey]Page, bp.numPages)
}

// _cleanUpTransaction releases all locks held by the transactions and removes the transaction from
// BufferPool data structures. We assume the calling method holds the mutex for the buffer pool.
func (bp *BufferPool) _cleanUpTransaction(tid TransactionID) {
	for lock := range bp.transactionLocks[tid] {
		if lock.perm == ReadPerm {
			bp.sharedLockMap[lock.pageKey] -= 1
		} else if lock.perm == WritePerm {
			delete(bp.exclusiveLockMap, lock.pageKey)
		}
	}
	delete(bp.transactionLocks, tid)
	delete(bp.transactionWaitingFor, tid)
}

// Abort the transaction, releasing locks. Because ailike is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	for lock := range bp.transactionLocks[tid] {
		if lock.perm == WritePerm {
			delete(bp.pageMap, lock.pageKey)
		}
	}
	bp._cleanUpTransaction(tid)
}

// Commit the transaction, releasing locks. Because ailike is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In ailike lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	for lock := range bp.transactionLocks[tid] {
		if lock.perm == WritePerm {
			// It is possible for a clean page to be evicted from the pageMap even if a transaction has an exclusive lock on it.
			// Therefore, we need this check.
			if page := bp.pageMap[lock.pageKey]; page != nil {
				err := bp.pageMap[lock.pageKey].flushPage()
				if err != nil {
					panic("Unable to flush page when commiting transaction. " + err.Error())
				}
			}
		}
	}
	bp._cleanUpTransaction(tid)
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	return nil
}

// // Used for debugging
// func (bp *BufferPool) printLockState() {
// 	fmt.Println("START STATE")
// 	fmt.Println("transaction waiting for: ")
// 	for k, v := range bp.transactionWaitingFor {
// 		fmt.Println(k, v)
// 	}
// 	fmt.Println("shared map: ")
// 	for k, v := range bp.sharedLockMap {
// 		fmt.Println(k, v)
// 	}
// 	fmt.Println("exclusive map: ")
// 	for k, v := range bp.exclusiveLockMap {
// 		fmt.Println(k, v)
// 	}
// 	fmt.Println("END STATE")
// }

// // Used for debugging
// // For each transaction in the cycle, print its:
// //   - tid
// //   - the lock it is waiting for
// //   - the locks it is currently holding/has reserved
// func (bp *BufferPool) printCycle(path []TransactionID, lasttid TransactionID) {
// 	fmt.Println(path, lasttid)
// 	for _, tid := range path {
// 		fmt.Println(tid, bp.transactionWaitingFor[tid], bp.transactionLocks[tid])
// 	}
// 	fmt.Println(lasttid, bp.transactionWaitingFor[lasttid], bp.transactionLocks[lasttid])
// }

// Checks if the given transaction is involved in any cycles in the wait-for graph.
func (bp *BufferPool) checkTransactionInCycle(tid TransactionID) bool {
	path := make([]TransactionID, 0)
	path = append(path, tid)
	visited := make(map[TransactionID]bool, 0)
	visited[tid] = true
	return bp._checkPathInCycle(path, visited)
}

// Checks if the given path is involved in any cycles in the wait-for graph.
// We do depth-first seach from the last transaction in the path. This method is called recursively
// until a cycle is detected involving the starting transaction, or the depth first search completes.
// Args:
// -- path: the current sequence of transactions we are exploring
// -- visited: a map keeping track of all transactions we have already visited
func (bp *BufferPool) _checkPathInCycle(path []TransactionID, visited map[TransactionID]bool) bool {
	curTid := path[len(path)-1]
	curLock := bp.transactionWaitingFor[curTid]
	curPageKey := curLock.pageKey
	curPerm := curLock.perm

	// Make a copy of the visited map
	newVisited := make(map[TransactionID]bool, len(visited))
	for k, v := range visited {
		newVisited[k] = v
	}

	// Check if another transaction is holding an exclusive lock on the desired pageKey.
	// It is possible for a transaction to be waiting for an exclusive lock that it already has "reserved";
	// if nextTid == curTid, that means we must be waiting another transaction to release a shared lock.
	nextTid, ok := bp.exclusiveLockMap[curPageKey]
	if ok && nextTid != nil && nextTid != curTid {
		// If nextTid is the first transaction in the path, we have found a cycle.
		if path[0] == nextTid {
			// bp.printCycle(path, tid)
			return true
		}
		// Otherwise, we explore nextTid's dependancies unless it was already visited.
		if _, ok := visited[nextTid]; !ok {
			if _, ok := bp.transactionWaitingFor[nextTid]; ok {
				pathCopy := make([]TransactionID, len(path))
				copy(pathCopy, path)
				pathCopy = append(pathCopy, nextTid)
				newVisited[nextTid] = true
				if bp._checkPathInCycle(pathCopy, newVisited) {
					return true
				}
			}
		}
	}
	// If we are waiting for an exclusive lock, then we also need to consider all the transactions holding shared locks.
	if curPerm == WritePerm {
		for nextTid, heldLocks := range bp.transactionLocks {
			if _, ok := heldLocks[Lock{curPageKey, ReadPerm}]; ok && nextTid != path[len(path)-1] {
				// If nextTid is the first transaction in the path, we have found a cycle.
				if path[0] == nextTid {
					// bp.printCycle(path, tid)
					return true
				}
				// Otherwise, we explore nextTid's dependancies unless it was already visited.
				if _, ok := visited[nextTid]; !ok {
					if _, ok := bp.transactionWaitingFor[nextTid]; ok {
						// We need to make a copy of visited.
						newPath := make([]TransactionID, len(path))
						copy(newPath, path)
						newPath = append(newPath, nextTid)
						newVisited[nextTid] = true
						if bp._checkPathInCycle(newPath, newVisited) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	pageKey := file.pageKey(pageNo)
	var attempts int = 0
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	// Add tid to the transactionLocks map if it is not already present
	if _, ok := bp.transactionLocks[tid]; !ok {
		bp.transactionLocks[tid] = make(map[Lock]bool)
	}

	var desiredLock Lock = Lock{pageKey, perm}
	bp.transactionWaitingFor[tid] = desiredLock

	// Block while another transaction is holding the exclusive lock for this page.
	for exclusiveLockHolder, ok := bp.exclusiveLockMap[pageKey]; ok && exclusiveLockHolder != nil; exclusiveLockHolder, ok = bp.exclusiveLockMap[pageKey] {
		// If the exclusive lock is being held by the current transaction, then we do not need to block.
		if exclusiveLockHolder == tid {
			break
		}
		// If the current transaction already holds a shared lock, then that means the exclusiveLockHoler is waiting
		// on the current transaction to release its shared lock so it can convert its "reservation" to a full lock.
		if _, ok := bp.transactionLocks[tid][desiredLock]; ok && perm == ReadPerm {
			break
		}

		attempts += 1
		if attempts%CYCLE_CHECK_INTERVAL == 0 {
			if bp.checkTransactionInCycle(tid) {
				if ABORT_TRANSACTIONS {
					bp.mutex.Unlock()
					bp.AbortTransaction(tid)
					time.Sleep(BLOCK_TIME)
					bp.mutex.Lock()
				}
				return nil, ailikeError{DeadlockError, "Deadlock detected."}
			}
		}
		bp.mutex.Unlock()
		time.Sleep(BLOCK_TIME)
		bp.mutex.Lock()
	}

	switch perm {
	case ReadPerm:
		if _, ok := bp.sharedLockMap[pageKey]; !ok {
			bp.sharedLockMap[pageKey] = 0
		}
		if _, ok := bp.transactionLocks[tid][desiredLock]; !ok {
			bp.transactionLocks[tid][desiredLock] = true
			bp.sharedLockMap[pageKey] += 1
		}
	case WritePerm:
		if ALLOW_RESERVATIONS {
			// Allow a transaction to "reserve" an exclusive lock to prevent new pages from acquiring shared locks.
			// The lock is not full "granted" until other transactions release their shared locks.
			bp.exclusiveLockMap[pageKey] = tid
			bp.transactionLocks[tid][desiredLock] = true
		}
		attempts = 0
		// Block until all transactions let go of shared locks.
		for numSharedHolders, ok := bp.sharedLockMap[pageKey]; ok && numSharedHolders > 0; numSharedHolders, ok = bp.sharedLockMap[pageKey] {
			_, transactionHoldsSharedLock := bp.transactionLocks[tid][Lock{pageKey, ReadPerm}]
			if numSharedHolders == 1 && transactionHoldsSharedLock {
				// If the only transaction holding a shared lock is this transaction, then we do not need to block.
				break
			}
			attempts += 1
			if attempts%CYCLE_CHECK_INTERVAL == 0 {
				if bp.checkTransactionInCycle(tid) {
					if ABORT_TRANSACTIONS {
						bp.mutex.Unlock()
						bp.AbortTransaction(tid)
						time.Sleep(BLOCK_TIME)
						bp.mutex.Lock()
					}
					return nil, ailikeError{DeadlockError, "Deadlock detected."}
				}
			}
			bp.mutex.Unlock()
			time.Sleep(BLOCK_TIME)
			bp.mutex.Lock()
		}
		if !ALLOW_RESERVATIONS {
			// If we do not allow reservations, then we wait until shared locks are released before acquiring exclusive locks.
			bp.exclusiveLockMap[pageKey] = tid
			bp.transactionLocks[tid][desiredLock] = true
		}

	}
	delete(bp.transactionWaitingFor, tid)

	if page, ok := bp.pageMap[pageKey]; ok {
		return &page, nil
	}

	page, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}

	if len(bp.pageMap) == bp.numPages {
		evictMethod := bp.EvictPage
		if USE_EVICT_QUEUE {
			evictMethod = bp.EvictPageQueue
		}
		if err := evictMethod(); err != nil {
			return nil, err
		}
	}

	bp.pageMap[pageKey] = *page
	bp.evictQueue = append(bp.evictQueue, pageKey)
	return page, nil
}

func (bp *BufferPool) hasPageCached(file DBFile, pageNo int, tid TransactionID, perm RWPerm) bool {
	pageKey := file.pageKey(pageNo)
	if _, ok := bp.pageMap[pageKey]; ok {
		return true
	}
	return false
}
