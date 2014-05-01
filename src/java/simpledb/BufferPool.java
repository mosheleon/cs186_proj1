package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
	/** Bytes per page, including header. */
	public static final int PAGE_SIZE = 4096;

	/** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
	public static final int DEFAULT_PAGES = 50;
	public static final long DEADLOCK_TIMEOUT = 100;
	private ArrayList<Page> myPool;
	private int maxPages;

	class LockId {
		Permissions type;
		TransactionId tid;
		//PageId pid;
		public LockId(Permissions type, TransactionId tid) {
			this.type = type;
			this.tid = tid;
		}
		
		public String toString() {
			return type.toString() + " " + tid.toString();
		}

		public boolean equals(Object lid) {
			LockId other = ((LockId)lid);
			//if (other.tid == null || tid == null)
				//return true;
			return (other.type).equals(type) && (other.tid).equals(tid);
		}

		public int hashCode() {
			return type.permLevel + tid.hashCode();
		}
	}

	//public LockManger lockManager = new LockManger(new HashMap<PageId, LinkedList<LockId>>(), 
	//new HashMap<TransactionId, LinkedList<LinkedList<LockId>>>());

	public HashMap<PageId, LinkedList<LockId>> lockManager = new HashMap<PageId, LinkedList<LockId>>();

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// some code goes here
		myPool = new ArrayList<Page>(numPages);
		maxPages = numPages;
	}

	/**
	 * Retrieve the specified page with the associated permissions.
	 * Will acquire a lock and may block if that lock is held by another
	 * transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool.  If it
	 * is present, it should be returned.  If it is not present, it should
	 * be added to the buffer pool and returned.  If there is insufficient
	 * space in the buffer pool, an page should be evicted and the new page
	 * should be added in its place.
	 *
	 * @param tid the ID of the transaction requesting the page 
	 * @param pid the ID of the requested page
	 * @param perm the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// some code goes here

		//HashMap<PageId, LinkedList<LockId>> pageLockMap = lockManager.pageLockMap;
		//HashMap<TransactionId, LinkedList<LinkedList<LockId>>> tidLockMap = lockManager.tidLockMap;
		LinkedList<LockId> holders;
		synchronized(lockManager) {
			holders = lockManager.get(pid);
		}
		if (holders != null) {
			boolean aborted = false;
			synchronized(holders) {
				if (perm.equals(Permissions.READ_ONLY)) {
					// can only be 1 write lock, check if that lock holder is not the same transaction
					long startTime = System.currentTimeMillis();
					long waitTime = 0;
					try {
						while (holders.size() == 1 
							//&& !holders.contains(new LockId(Permissions.READ_WRITE, tid))
							//&& !holders.contains(new LockId(Permissions.READ_ONLY, tid)) ) {
							&& holders.getFirst().type.equals(Permissions.READ_WRITE)
							//&& holders.getFirst().tid != null
							&& !holders.getFirst().tid.equals(tid)) {
							holders.wait(DEADLOCK_TIMEOUT); // wait for write lock held by another transaction to be released
							waitTime = (new Date()).getTime() - startTime;
							if (waitTime > this.DEADLOCK_TIMEOUT) {
								//System.out.println("trying to get : " + (new LockId(perm, tid)).toString() + "TIMEOUT while waiting for : " + holders.getFirst().toString());
								aborted = true;
								break;
							}
						}
					} catch (InterruptedException e) {
						try {
							transactionComplete(tid, false);
						} catch (IOException e1) {
							e1.printStackTrace();
						}
						throw new TransactionAbortedException();
					}
					LockId lid = new LockId(perm, tid);
					if ( !aborted && !holders.contains(lid) && !holders.contains(new LockId(Permissions.READ_WRITE, tid))) {
						//System.out.println(tid.toString() + " acquired : " + lid.toString());
						holders.add(lid); // add tid to read lock holder list (shared lock) if tid doesn't already have lock
					}
				}
				else {
					LockId lid = new LockId(perm, tid);
					//System.out.println("holders : " + holders.toString());
					if (holders.size() == 1 && holders.contains(new LockId(Permissions.READ_ONLY, tid))) {
						//System.out.println("upgraded to lock : " + lid.toString());
						holders.remove(new LockId(Permissions.READ_ONLY, tid));
						holders.add(lid); // upgrade lock
					}
					long startTime = System.currentTimeMillis();
					long waitTime = 0;
					try {
						while (holders.size() > 0 && !holders.contains(lid)) {
							if (holders.size() == 1 && holders.contains(new LockId(Permissions.READ_ONLY, tid))) {
								//System.out.println("upgraded to lock : " + lid.toString());
								holders.remove(new LockId(Permissions.READ_ONLY, tid));
								holders.add(lid); // upgrade lock
								break;
							}
							holders.wait(DEADLOCK_TIMEOUT); // wait for access of exclusive lock
							waitTime = (new Date()).getTime() - startTime;
							if (waitTime > this.DEADLOCK_TIMEOUT) {
								//System.out.println("trying to get : " + (new LockId(perm, tid)).toString() + " for page : " + pid.pageNumber());
								//System.out.println(tid.toString() + " TIMEOUT while waiting for : " + holders.toString() + " holding for page : " + pid.pageNumber());
								aborted = true;
								break;
							}
						}
					} catch (InterruptedException e) {
						try {
							transactionComplete(tid, false);
						} catch (IOException e1) {
							e1.printStackTrace();
						}
						throw new TransactionAbortedException();
					}
					if (!aborted && !holders.contains(lid)) {
						//System.out.println(tid.toString() + " acquired : " + lid.toString());
						holders.add(lid); // add tid to write lock holder list (exclusive lock)
					}
				}
			}
			if (aborted) {
				try {
					transactionComplete(tid, false);
				} catch (IOException e) {
				}
				throw new TransactionAbortedException();
			}
		}
		synchronized(lockManager) { // create new lock and acquire it
			if (holders == null) {
				holders = new LinkedList<LockId>();
				holders.add(new LockId(perm, tid));
				lockManager.put(pid, holders);
			}
		} // end synchronized block

		for (int i = 0; i < myPool.size(); i ++ ) {
			Page p = myPool.get(i);
			if (p != null && pid.equals(p.getId())) {
				return p;
			}
		}

		Catalog globalCatalog = Database.getCatalog();
		int tableId = pid.getTableId();
		DbFile f = globalCatalog.getDbFile(tableId);
		Page p = f.readPage(pid);
		if (myPool.size() == maxPages) {
			evictPage();
		}
		myPool.add(p);
		return p;
	}

	public void startTimer() throws TransactionAbortedException {
		long start = System.currentTimeMillis();
		long waitTime = 0;
		while(waitTime < DEADLOCK_TIMEOUT) {

		}
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
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for proj1
		LinkedList<LockId> holders;
		synchronized(lockManager) {
			holders = lockManager.get(pid);
		}
		//System.out.println("tid " + tid.toString() + " releasing page : " + pid.pageNumber());
		if (holders != null) {
			synchronized(holders) {
				for (int i = 0; i < holders.size(); i++) {
					if (holders.get(i).tid.equals(tid)) {
						LockId removed = holders.remove(i);
						//System.out.println("removed lock for : " + removed.toString());
					}
				}
				holders.notifyAll(); // wake waiting threads
			}
		}
		//System.out.println("tid " + tid.toString() + " released page : " + pid.pageNumber());
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for proj1
		transactionComplete(tid, true);
	}

	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for proj1
		LinkedList<LockId> holders;
		synchronized(lockManager) {
			holders = lockManager.get(p);
		}
		if (holders != null) {
			synchronized(holders) {
				for (int i = 0; i < holders.size(); i++) {
					if (holders.get(i).tid.equals(tid)) {
						return true;
					}
				}
			}
		}
		return false;
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
		// not necessary for proj1
		if (commit)
			flushPages(tid);
		else {
			for (int i = 0; i < myPool.size(); i++) {
				Page p = myPool.get(i);
				TransactionId dirtyPageTid = p.isDirty();
				if (dirtyPageTid != null && dirtyPageTid.equals(tid)) {
					int tableid = p.getId().getTableId();
					Page pageFromDisk = Database.getCatalog().getDbFile(tableid).readPage(p.getId());
					myPool.set(i, pageFromDisk);
				}
			}
			//System.out.println(tid.toString() + " aborted");
		}
		for (Iterator<PageId> it = lockManager.keySet().iterator(); it.hasNext();) {
			PageId pid = it.next();
			releasePage(tid, pid);
		}
		//System.out.println(tid.toString() + " released pages");
	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid.  Will
	 * acquire a write lock on the page the tuple is added to(Lock 
	 * acquisition is not needed for lab2). May block if the lock cannot 
	 * be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and updates cached versions of any pages that have 
	 * been dirtied so that future requests see up-to-date pages. 
	 *
	 * @param tid the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		HeapFile f = (HeapFile) Database.getCatalog().getDbFile(tableId);
		ArrayList<Page> modifiedPages = f.insertTuple(tid, t);
	}

	/**
	 * Remove the specified tuple from the buffer pool.
	 * Will acquire a write lock on the page the tuple is removed from. May block if
	 * the lock cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit.  Does not need to update cached versions of any pages that have 
	 * been dirtied, as it is not possible that a new page was created during the deletion
	 * (note difference from addTuple).
	 *
	 * @param tid the transaction adding the tuple.
	 * @param t the tuple to add
	 */
	public void deleteTuple(TransactionId tid, Tuple t)
			throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		int tableId = t.getRecordId().getPageId().getTableId();
		Database.getCatalog().getDbFile(tableId).deleteTuple(tid, t);
	}

	private int findPage(PageId pid) {
		for (int i = 0; i < myPool.size(); i ++ ) {
			Page p = myPool.get(i);
			if (pid != null && pid.equals(p.getId()))
				return i;
		}
		return -1;
	}
	/**
	 * Flush all dirty pages to disk.
	 * NB: Be careful using this routine -- it writes dirty data to disk so will
	 *     break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for proj1
		for (int i = 0; i < myPool.size(); i ++ ) {
			Page p = myPool.get(i);
			if (p.isDirty() != null) {
				flushPage(p.getId());
				p.markDirty(false, p.isDirty());
			}
		}
	}

	/** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for proj1
		int index = findPage(pid);
		if (index >= 0)
			myPool.remove(index);
	}

	/**
	 * Flushes a certain page to disk
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized  void flushPage(PageId pid) throws IOException {
		// some code goes here
		// not necessary for proj1
		int i = findPage(pid);
		if (i >= 0 ) {
			int tableid = pid.getTableId();
			Page p = myPool.get(i);
			HeapFile f = (HeapFile) Database.getCatalog().getDbFile(tableid);
			f.writePage(p);
		}
	}

	/** Write all pages of the specified transaction to disk.
	 */
	public synchronized  void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for proj1
		for (int i = 0; i < myPool.size(); i ++ ) {
			Page p = myPool.get(i);
			TransactionId t = p.isDirty();
			if (t != null && t.equals(tid)) {
				flushPage(p.getId());
				p.markDirty(false, tid);
			}
		}
	}

	/**
	 * Discards a page from the buffer pool.
	 * Flushes the page to disk to ensure dirty pages are updated on disk.
	 */
	private synchronized  void evictPage() throws DbException {
		// some code goes here
		// not necessary for proj1
		//		try {	
		//			Page p = myPool.get(0);
		//			TransactionId tid = p.isDirty();
		//			if (tid != null) {
		//				flushPage(p.getId());
		//				p.markDirty(false, tid);
		//			}
		//			myPool.remove(0);
		//		} catch (IOException e) {
		//			System.out.println("There was an IOException " + e);
		//		}

		for (int i = 0; i < myPool.size(); i++) {
			Page p = myPool.get(i);
			TransactionId tid = p.isDirty();
			if (tid == null) {
				myPool.remove(i);
				return;
			}
		}
		throw new DbException("all pages are dirty, can't evict");


	}

}
