package simpledb;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	private static class LockManager {
		private static HashMap<PageId, TransactionId> exclusiveLocks;
		private static HashMap<PageId, HashSet<TransactionId>> readLocks;
		private static HashMap<TransactionId, HashSet<TransactionId>> waitList;
		
		public LockManager() {
			exclusiveLocks = new HashMap<>();
			readLocks = new HashMap<>();
			waitList = new HashMap<>();
		}
		
		public synchronized boolean isDeadLock(TransactionId tid, PageId pid) {
			if(exclusiveLocks.containsKey(pid) && !exclusiveLocks.get(pid).equals(tid)) {
				if(waitList.containsKey(tid)) {
					if(waitList.get(tid).contains(exclusiveLocks.get(pid))) {
						// Dead locks exists because both transactions waiting
						// for lock from the other and none release 
						return true;
					}
				}
				TransactionId tidWithLock = exclusiveLocks.get(pid);
				if(waitList.containsKey(tidWithLock)) {
					HashSet<TransactionId> waitingTransactions = waitList.get(tidWithLock);
					waitingTransactions.add(tid);
				}
				waitList.put(tidWithLock, new HashSet<TransactionId>());
			}
			return false;
		}
		
		public synchronized boolean isWaiting(TransactionId mainTrans, TransactionId waitTrans) {
			if(waitList.containsKey(mainTrans)&& waitList.get(mainTrans).contains(waitTrans)) {
				return true;
			}
			return false;
		}
		
		public synchronized boolean addToWaitList(TransactionId mainTrans, TransactionId waitTrans) {
			if(isWaiting(mainTrans, waitTrans)) {
				return false;
			}
			if(!waitList.containsKey(mainTrans)) {
				waitList.put(mainTrans, new HashSet<TransactionId>());
			}
			HashSet<TransactionId> waitingTids = waitList.get(mainTrans);
			waitingTids.add(waitTrans);
			return true;
		}
		
		public synchronized void deletFromWaitList(TransactionId removeTrans) {
			waitList.remove(removeTrans);
			for(HashSet<TransactionId> waiting : waitList.values()) {
				waiting.remove(removeTrans);
			}
		}
		
		public synchronized void releaseTransaction(TransactionId tid) {
			for(PageId pid : exclusiveLocks.keySet()) {
				if(exclusiveLocks.get(pid).equals(tid)) {
					exclusiveLocks.remove(pid);
				}
			}
			for(PageId pid : readLocks.keySet()) {
				readLocks.get(pid).remove(tid);
			}
		}
		
		public synchronized boolean getReadLock(PageId pid, TransactionId tid) throws TransactionAbortedException{
			if(exclusiveLocks.containsKey(pid)) {
				if(tid.equals(exclusiveLocks.get(pid))) {
					if(readLocks.containsKey(pid)) {
						readLocks.get(pid).add(tid);
					} else {
						readLocks.put(pid, new HashSet<TransactionId>());
						readLocks.get(pid).add(tid);
					}
					return true;
				} else {
					if(!addToWaitList(exclusiveLocks.get(pid), tid)) {
						//throw new TransactionAbortedException();
					}
					return false;
				}
			} else {
				if(readLocks.containsKey(pid)) {
					readLocks.get(pid).add(tid);
				} else {
					readLocks.put(pid, new HashSet<TransactionId>());
					readLocks.get(pid).add(tid);
				}
				return true;
			}
		}
		
		public synchronized boolean getExclusiveLock(PageId pid, TransactionId tid) throws TransactionAbortedException{
			if(exclusiveLocks.containsKey(pid) && !exclusiveLocks.get(pid).equals(tid)) {
				if(addToWaitList(exclusiveLocks.get(pid), tid)) {
					throw new TransactionAbortedException();
				}
				return false;
			}
			
			if(readLocks.containsKey(pid)) {
				HashSet<TransactionId> transactions = readLocks.get(pid);
				if(transactions.size() != 0) {
					if(transactions.size() == 1) {
						if(transactions.contains(tid)) {
							if(isDeadLock(tid, pid)) {
								throw new TransactionAbortedException();
							}
							exclusiveLocks.put(pid, tid);
							return true;
						} else {
							if(!addToWaitList(readLocks.get(pid).iterator().next(), tid)) {
								//throw new TransactionAbortedException();
							}
							return false;
						}
					} else {
						Iterator<TransactionId> tidIter = readLocks.get(pid).iterator();
						while(tidIter.hasNext()) {
							if(!addToWaitList(tidIter.next(), tid)) {
								throw new TransactionAbortedException();
							}
						}
						return false;
					}
				} else {
					if(isDeadLock(tid, pid)) {
						throw new TransactionAbortedException();
					}
					exclusiveLocks.put(pid, tid);
					return true;
				}
			} else {
				if(isDeadLock(tid, pid)) {
					throw new TransactionAbortedException();
				}
				exclusiveLocks.put(pid, tid);
				return true;
				
			}
		}
		
		public synchronized boolean removeReadLock(PageId pid, TransactionId tid) {
			if(!readLocks.containsKey(pid)) {
				return false;
			}
			return readLocks.get(pid).remove(tid);
		}
		
		public synchronized boolean removeExclusiveLock(PageId pid, TransactionId tid) {
			if(!exclusiveLocks.containsKey(pid)) {
				return false;
			} else {
				if(exclusiveLocks.get(pid).equals(tid)) {
					exclusiveLocks.remove(pid);
					return true;
				} else {
					return false;
				}
			}
		}
		
		public synchronized boolean hasReadLock(PageId pid, TransactionId tid) {
			return readLocks.containsKey(pid) && readLocks.get(pid).contains(tid);
		}
		
		public synchronized boolean hasExclusiveLock(PageId pid, TransactionId tid) {
			return exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid);
		}
		
	}
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public ConcurrentHashMap<PageId, Page> setofPages;
    public int capacity;
    public Deque<PageId> deque;
    LockManager lockManag;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.setofPages = new ConcurrentHashMap<>();
        this.capacity = numPages;
        deque = new ArrayDeque<>();
        lockManag = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	
    	boolean hasLock = false;
    	while(!hasLock) {
    		synchronized(this) {
    			if(perm == Permissions.READ_WRITE) {
    				hasLock = lockManag.getExclusiveLock(pid, tid);
    			} else {
    				hasLock = lockManag.getReadLock(pid, tid);
    			}
    		}
    		if(!hasLock) {
    			try {
    				Thread.sleep(1);
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    		}
    	}
        // checks whether this page is present in BufferPoll
        if (setofPages.keySet().contains(pid) && setofPages.get(pid) != null) {
            // move to the front of the queue. Most recently used
            deque.remove(pid);
            deque.addLast(pid);
            return setofPages.get(pid);
        }

        // some code goes here
        if (setofPages.size() >= capacity ) {
            evictPage();
        }

        // Gets the tableID contains this page
        int tableId = pid.getTableId();

        // Add the page the BufferPool and return
        Page page = Database.getCatalog().idToFile.get(tableId).readPage(pid);
        setofPages.put(pid, page);
        deque.addLast(pid);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	lockManag.removeExclusiveLock(pid, tid);
    	lockManag.removeReadLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManag.hasExclusiveLock(p, tid) || lockManag.hasReadLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // Get the HeapFile from tableId
        HeapFile hf = (HeapFile) Database.getCatalog().idToFile.get(tableId);
        
        // Insert tuple t into this hf file.
        ArrayList<Page> li = hf.insertTuple(tid, t);
        for (Page page : li) {
            page.markDirty(true, tid);

            // replace this dirty page in Buffer Poll
            setofPages.put(page.getId(), page);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // retrive the file base on info from tuple t
        int tableId =  t.getRecordId().getPageId().getTableId();
        HeapFile hf = (HeapFile) Database.getCatalog().idToFile.get(tableId);

        // delete the tuple from the hf file
        ArrayList<Page> li = hf.deleteTuple(tid, t);
        // Mark it dirty and replace it in Buffer Pool
        for (Page page : li) {
            page.markDirty(true, tid);

            // replace this dirty page in Buffer Poll
            setofPages.put(page.getId(), page);
        }
        
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId : setofPages.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	this.setofPages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        // Get the page from Pageid
        HeapPage page = (HeapPage) setofPages.get(pid);

        if (page == null) {
            throw new IOException("The page is not in Buffer Pool");
        }

        // write this page to disk
        Database.getCatalog().idToFile.get(pid.getTableId()).writePage(page);

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Iterator<PageId> iter = deque.iterator();
        while (iter.hasNext()) {
            PageId pageId = iter.next();
            // Get the first not dirty page
            if (setofPages.get(pageId).isDirty() == null) {
                deque.remove(pageId);
                // remove from the Hash
                setofPages.remove(pageId);
                break;
            }    
        }
        if (setofPages.size() >= this.capacity) {
            throw new DbException("Buffer Pool is full and can't be evict");
        }
    }

}
