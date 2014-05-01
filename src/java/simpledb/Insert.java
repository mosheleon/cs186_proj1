package simpledb;

import java.io.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

	private static final long serialVersionUID = 1L;
	private TransactionId tid;
	private DbIterator child;
	private int tableid;
	private int fetchNextCalled;
	private TupleDesc td;
	
	/**
	 * Constructor.
	 * 
	 * @param t
	 *            The transaction running the insert.
	 * @param child
	 *            The child operator from which to read tuples to be inserted.
	 * @param tableid
	 *            The table in which to insert tuples.
	 * @throws DbException
	 *             if TupleDesc of child differs from table into which we are to
	 *             insert.
	 */
	public Insert(TransactionId t,DbIterator child, int tableid)
			throws DbException {
		// some code goes here
		this.child = child;
		this.tid =  t;
		this.tableid = tableid;
		this.fetchNextCalled = 0;
		Type[] typ = new Type[1];
		typ[0] = Type.INT_TYPE;
		this.td = new TupleDesc(typ);
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return td;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		super.open();
		child.open();
	}

	public void close() {
		// some code goes here
		super.close();
		child.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		open();
		close();
	}

	/**
	 * Inserts tuples read from child into the tableid specified by the
	 * constructor. It returns a one field tuple containing the number of
	 * inserted records. Inserts should be passed through BufferPool. An
	 * instances of BufferPool is available via Database.getBufferPool(). Note
	 * that insert DOES NOT need check to see if a particular tuple is a
	 * duplicate before inserting it.
	 * 
	 * @return A 1-field tuple containing the number of inserted records, or
	 *         null if called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (fetchNextCalled==1)
			return null;
		fetchNextCalled = 1;
		int count = 0;
		try {
		while(child.hasNext()) {
			Tuple t = child.next();
			Database.getBufferPool().insertTuple(tid, tableid, t);
			count ++;
		}

		Tuple result = new Tuple(td);
		result.setField(0, new IntField(count));
		return result;
		} catch (IOException e) {
			throw new DbException("can't insert tuple");
		}
	}

	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		DbIterator[] children = new DbIterator[1];
		children[0] = child;
		return children;
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		child = children[0];
	}
}
