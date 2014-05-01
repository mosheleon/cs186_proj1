package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private final PageId _myPageId; //final because no set method for pid
    private final int _myTupleNumber; //final because no set method for tupleno
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
    	_myPageId = pid;
    	_myTupleNumber = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return _myTupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return _myPageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
    	try {
    	    if ((_myPageId.equals( ( (RecordId) o).getPageId() ) && _myTupleNumber == ((RecordId) o).tupleno())) {
    		    return true;
    	    }
    	} catch (Exception e) {
    		return false;
    	}
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() { // use prime hash
        // some code goes here
    	int seed = 13;
    	int hc = 1;
    	hc = hc*seed + _myPageId.hashCode();
    	hc = hc*seed + _myTupleNumber;
        return hc;

    }

}
