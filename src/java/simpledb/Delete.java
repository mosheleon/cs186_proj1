package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

	private static final long serialVersionUID = 1L;
	private TransactionId tid;
	private DbIterator child;
	private int fetchNextCalled;
	private TupleDesc td;

	/**
	 * Constructor specifying the transaction that this delete belongs to as
	 * well as the child to read from.
	 * 
	 * @param t
	 *            The transaction this delete runs in
	 * @param child
	 *            The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, DbIterator child) {
		// some code goes here
		this.child = child;
		this.tid =  t;
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
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 * 
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (fetchNextCalled==1)
			return null;
		fetchNextCalled = 1;
		int count = 0;
		while(child.hasNext()) {
			Tuple t = child.next();
			Database.getBufferPool().deleteTuple(tid, t);
			count ++;
		}

		Tuple result = new Tuple(td);
		result.setField(0, new IntField(count));
		return result;
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
