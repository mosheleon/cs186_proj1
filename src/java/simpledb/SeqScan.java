package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId transid;
    private int tableid;
    private String tableAlias;
    private DbFile file;
    private DbFileIterator dbFileItr;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	this.transid = tid;
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    	this.file = Database.getCatalog().getDbFile(tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
    	return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias(){
    	return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    	// What should I do with transactionId ?
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    	//might omit this later
    	this.file = Database.getCatalog().getDbFile(tableid);
    	
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        //might change this later
    	this.file = Database.getCatalog().getDbFile(tableid);
    	dbFileItr = this.file.iterator(transid);
    	dbFileItr.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
    	Type[] typeArr = new Type[td.numFields()];
    	String[] nameArr = new String[td.numFields()];
    	for(int i = 0 ; i < td.numFields() ; i++){
    		typeArr[i] = td.getFieldType(i);
    		nameArr[i] = getAlias() + "." + td.getFieldName(i);
    	}
    	return new TupleDesc(typeArr, nameArr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	try{ 
    		return dbFileItr.hasNext();
    	}
    	catch(Exception e){
    		System.out.print("There was an exception: " + e);
    		throw new TransactionAbortedException();
    	}
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	if(hasNext()){
    		return dbFileItr.next();
    	}
    	else{
    		return null;
    	}
    }

    public void close() {
        // some code goes here
    	dbFileItr.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    	dbFileItr.rewind();
    }
}
