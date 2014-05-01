package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private File _myFile;
	private TupleDesc _myTupleDesc;
	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td) {
		// some code goes here
		_myFile = f;
		_myTupleDesc = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		// some code goes here
		return _myFile;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		// some code goes here
		return _myFile.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return _myTupleDesc;
	}

	// see DbFile.java for javadocs 

	public Page readPage(PageId pid) {
		int pageNumber = pid.pageNumber();
		int offset = BufferPool.PAGE_SIZE * pageNumber;
		HeapPage toReturn = null;
		RandomAccessFile raf = null;
		try{
			synchronized(_myFile) {
				raf = new RandomAccessFile(_myFile, "r");
				byte[] pageData = new byte[BufferPool.PAGE_SIZE];
				raf.seek(offset);
				raf.read(pageData, 0, BufferPool.PAGE_SIZE);
				toReturn = new HeapPage((HeapPageId) pid, pageData);
				raf.close();
			}
			return toReturn;
		}
		catch(Exception e){
			e.printStackTrace();
			throw new IllegalArgumentException("can't find page");
		}
	}


	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for proj1
		try {
			synchronized(_myFile){
				int offset = BufferPool.PAGE_SIZE * page.getId().pageNumber();
				RandomAccessFile raf = new RandomAccessFile(_myFile, "rw");
				byte[] pageData = page.getPageData();
				raf.seek(offset);
				raf.write(pageData);
			}
		} catch(Exception e) {
			System.out.println(e);
			throw new IOException("can't write page to file");
		}

	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		// some code goes here
		return ((int) _myFile.length()) / BufferPool.PAGE_SIZE;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		ArrayList<Page> modifiedPages = new ArrayList<Page>();
		try {
			boolean insertedTuple = false;
			for (int i = 0; i < numPages(); i ++) {
				HeapPageId pid = new HeapPageId(getId(), i);
				HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
				if (p.getNumEmptySlots() == 0) {
					Database.getBufferPool().releasePage(tid, pid);
				}
				if (p.getNumEmptySlots() >  0) {
					Database.getBufferPool().releasePage(tid, pid);
					p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
					p.insertTuple(t);
					insertedTuple = true;
					p.markDirty(true, tid);
					modifiedPages.add(p);
					break;
				}
			}
//			if (!insertedTuple) {
//				synchronized(_myFile) {
//					RandomAccessFile raf = new RandomAccessFile(_myFile, "rw");
//					byte[] pageData = HeapPage.createEmptyPageData();
//					raf.seek(BufferPool.PAGE_SIZE * (numPages()));
//					HeapPageId pid = new HeapPageId(getId(), numPages()+1);
//					HeapPage p = new HeapPage(pid, pageData);
//					p.insertTuple(t);
//					p.markDirty(true, tid);
//					raf.write(p.getPageData());
//					raf.close();
//					modifiedPages.add(p);
//				}
//			}
			
			if (!insertedTuple) {
				HeapPageId pid;
				HeapPage p;
				synchronized(_myFile) {
					RandomAccessFile raf = new RandomAccessFile(_myFile, "rw");
					byte[] pageData = HeapPage.createEmptyPageData();
					raf.seek(BufferPool.PAGE_SIZE * (numPages()));
					pid = new HeapPageId(getId(), numPages());
					p = new HeapPage(pid, pageData);
					raf.write(p.getPageData());
					raf.close();
				}
				p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
				p.insertTuple(t);
				p.markDirty(true, tid);
				modifiedPages.add(p);
			}

		} catch (DbException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} catch (TransactionAbortedException e) {
			throw e;
		}
		return modifiedPages;
		// not necessary for proj1
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
	TransactionAbortedException {
		// some code goes here
		try {
			HeapPageId pid = (HeapPageId) t.getRecordId().getPageId();
			if (pid.getTableId() != getId())
				throw new DbException("wrong tuple tableid");
			HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			p.deleteTuple(t);
			p.markDirty(true, tid);
			return p;
		} catch (DbException e) {
			throw e;
		} catch (TransactionAbortedException e) {
			throw e;
		} catch (NullPointerException e) {
			throw new DbException("invalid tuple argument");
		}
		// not necessary for proj1
	}

	class HFIterator implements DbFileIterator {
		private File _myFile;
		private int _myPageIndex = 0;
		private int _myTupleIndex = 0;
		private int _myTableId;
		private HeapPage _myPage;
		private Iterator<Tuple> _myTupleIterator;
		private String _myStatus = "closed"; 
		private TransactionId _myTransactionId;

		public HFIterator(File f, int tableId, TransactionId tid) {
			_myFile = f;
			_myTableId = tableId;
			_myStatus = "open";
			_myTransactionId = tid;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			try {
				_myPageIndex = 0;
				_myTupleIndex = 0;
				_myStatus = "open";
				HeapPageId hpid = new HeapPageId(_myTableId, _myPageIndex);
				_myPage = (HeapPage) Database.getBufferPool().getPage(_myTransactionId, hpid, Permissions.READ_ONLY);
				_myTupleIterator = _myPage.iterator();

			} catch (Exception e) {
				//e.printStackTrace();
				throw new DbException("can't open HeapFileIterator");
			}
		}

		@Override
		public boolean hasNext() throws DbException,
		TransactionAbortedException {
			if (_myStatus == "closed")
				throw new NoSuchElementException();
			if (_myTupleIterator != null && _myTupleIterator.hasNext())
				return true;
			else
				return false;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
		NoSuchElementException {
			if (_myStatus == "closed")
				throw new NoSuchElementException();
			if (!hasNext())
				throw new NoSuchElementException("no more tuples");
			Tuple t = _myTupleIterator.next();
			_myTupleIndex++;
			if (_myTupleIndex == _myPage.numSlots) {
				_myPageIndex++;
				_myTupleIndex = 0;
				HeapPageId hpid = new HeapPageId(_myTableId, _myPageIndex);
				if (_myPageIndex != numPages()) {
					_myPage = (HeapPage) Database.getBufferPool().getPage(_myTransactionId, hpid, Permissions.READ_ONLY);
					_myTupleIterator = _myPage.iterator();
				}
			}
			return t;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			if (_myStatus == "closed")
				throw new NoSuchElementException();
			open();
		}

		@Override
		public void close() {
			_myStatus = "closed";
		}


	}
	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		return new HFIterator(_myFile, getId(), tid);
	}
}

