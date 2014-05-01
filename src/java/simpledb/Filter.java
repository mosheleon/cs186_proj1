package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    //The predicate to filter tuples with
    private Predicate p;
    //The child operator
    private DbIterator child;
    private int card=0;
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	this.p = p;
    	this.child = child;    	
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	TupleDesc td = child.getTupleDesc();
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	//super- opens the Iterator- this must be called before
    	//                             any of the other methods
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	//Maintain same order as in open()
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	while(child.hasNext()){
    		Tuple temp = child.next();
    		boolean answer = p.filter(temp);
    		if(answer){
    			return temp;
    		}
    	}
        return null;
    }

    /**
     * @return return the children DbIterators of this operator. If there is
     *         only one child, return an array of only one element. For join
     *         operators, the order of the children is not important. But they
     *         should be consistent among multiple calls.
     * */
    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	//Making an array with enough room for 1 element
    	DbIterator[] toReturn = new DbIterator[1];
    	toReturn[0] = child;
        return toReturn;
    }

    /**
     * Set the children(child) of this operator. If the operator has only one
     * child, children[0] should be used. If the operator is a join, children[0]
     * and children[1] should be used.
     * 
     * 
     * @param children
     *            the DbIterators which are to be set as the children(child) of
     *            this operator
     * */
    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	child = children[0];    	
    }

}
