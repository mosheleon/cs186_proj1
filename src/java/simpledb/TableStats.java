package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

	private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();
	private HashMap<Integer, IntHistogram> intH = new HashMap<Integer, IntHistogram>();
	private HashMap<Integer, StringHistogram> stringH = new HashMap<Integer, StringHistogram>();
	private int tableid;
	private int ioCostPerPage = IOCOSTPERPAGE;
	private int numPages;
	private int numTuples;
	public TupleDesc td;
	public int[] distinctValues;
	
	static final int IOCOSTPERPAGE = 1000;

	public static TableStats getTableStats(String tablename) {
		return statsMap.get(tablename);
	}

	public static void setTableStats(String tablename, TableStats stats) {
		statsMap.put(tablename, stats);
	}

	public static void setStatsMap(HashMap<String,TableStats> s)
	{
		try {
			java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
			statsMapF.setAccessible(true);
			statsMapF.set(null, s);
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

	}

	public static Map<String, TableStats> getStatsMap() {
		return statsMap;
	}

	public static void computeStatistics() {
		Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

		System.out.println("Computing table stats.");
		while (tableIt.hasNext()) {
			int tableid = tableIt.next();
			TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
			setTableStats(Database.getCatalog().getTableName(tableid), s);
		}
		System.out.println("Done.");
	}

	/**
	 * Number of bins for the histogram. Feel free to increase this value over
	 * 100, though our tests assume that you have at least 100 bins in your
	 * histograms.
	 */
	static final int NUM_HIST_BINS = 100;

	/**
	 * Create a new TableStats object, that keeps track of statistics on each
	 * column of a table
	 * 
	 * @param tableid
	 *            The table over which to compute statistics
	 * @param ioCostPerPage
	 *            The cost per page of IO. This doesn't differentiate between
	 *            sequential-scan IO and disk seeks.
	 */
	public TableStats(int tableid, int ioCostPerPage) {
		// For this function, you'll have to get the
		// DbFile for the table in question,
		// then scan through its tuples and calculate
		// the values that you need.
		// You should try to do this reasonably efficiently, but you don't
		// necessarily have to (for example) do everything
		// in a single scan of the table.
		// some code goes here

		this.tableid = tableid;
		this.ioCostPerPage = ioCostPerPage;

		DbFile table = Database.getCatalog().getDbFile(tableid);
		this.td = table.getTupleDesc();
		this.numTuples = 0;
		int tupleSize = td.getSize();
		this.numPages = 0;
		LinkedList<Integer> uniquePages = new LinkedList<Integer>();
		//first scan to find min and max and number of distinct values for each field
		TransactionId tid = new TransactionId();
		distinctValues = new int[td.numFields()];
		int[][] min_max = new int[td.numFields()][2];
		for (int i = 0; i < td.numFields(); i++) {
			min_max[i][0] = Integer.MAX_VALUE;
			min_max[i][1] = Integer.MIN_VALUE;
		}
		DbFileIterator it = table.iterator(tid);
		try {
			it.open();
			while( it.hasNext() ){
				Tuple t = it.next();
				if (!uniquePages.contains(t.getRecordId().getPageId().pageNumber()))
					uniquePages.add(t.getRecordId().getPageId().pageNumber());
				numTuples++;
				for (int i = 0; i < td.numFields(); i++) {
					if (td.getFieldType(i) == Type.INT_TYPE) {
						min_max[i][0] = Math.min(min_max[i][0], ((IntField)(t.getField(i))).getValue());
						min_max[i][1] = Math.max(min_max[i][1], ((IntField)(t.getField(i))).getValue());
					}
				}
			}
			this.numPages = uniquePages.size();
			//int tupsPerPage = (BufferPool.PAGE_SIZE * 8) / (td.getSize() * 8 + 1);
			//this.numPages = numTuples*tupleSize/BufferPool.PAGE_SIZE;
			// construct histograms for each field
			for (int i = 0; i < td.numFields(); i++) {
				if(td.getFieldType(i) == Type.INT_TYPE) 
					intH.put(i, new IntHistogram(NUM_HIST_BINS, min_max[i][0], min_max[i][1]));
				else
					stringH.put(i, new StringHistogram(NUM_HIST_BINS));
			}

			// second scan to populate the histograms
			it.close();
			it = table.iterator(tid);
			it.open();
			while( it.hasNext() ){
				Tuple t = it.next();
				for (int i = 0; i < td.numFields(); i++) {
					if (td.getFieldType(i) == Type.INT_TYPE) {
						intH.get(i).addValue(((IntField)t.getField(i)).getValue());
					}
					else {
						stringH.get(i).addValue(((StringField)t.getField(i)).getValue());
					}
				}
			}
			it.close();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Estimates the cost of sequentially scanning the file, given that the cost
	 * to read a page is costPerPageIO. You can assume that there are no seeks
	 * and that no pages are in the buffer pool.
	 * 
	 * Also, assume that your hard drive can only read entire pages at once, so
	 * if the last page of the table only has one tuple on it, it's just as
	 * expensive to read as a full page. (Most real hard drives can't
	 * efficiently address regions smaller than a page at a time.)
	 * 
	 * @return The estimated cost of scanning the table.
	 */
	public double estimateScanCost() {
		// some code goes here
		//return this.numPages * this.ioCostPerPage;
		return this.numPages*this.ioCostPerPage;
	}

	/**
	 * This method returns the number of tuples in the relation, given that a
	 * predicate with selectivity selectivityFactor is applied.
	 * 
	 * @param selectivityFactor
	 *            The selectivity of any predicates over the table
	 * @return The estimated cardinality of the scan with the specified
	 *         selectivityFactor
	 */
	public int estimateTableCardinality(double selectivityFactor) {
		// some code goes here
		return (int)(this.numTuples*selectivityFactor);
	}

	/**
	 * The average selectivity of the field under op.
	 * @param field
	 *        the index of the field
	 * @param op
	 *        the operator in the predicate
	 * The semantic of the method is that, given the table, and then given a
	 * tuple, of which we do not know the value of the field, return the
	 * expected selectivity. You may estimate this value from the histograms.
	 * */
	public double avgSelectivity(int field, Predicate.Op op) {
		// some code goes here
		return 1.0;
	}

	/**
	 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
	 * table.
	 * 
	 * @param field
	 *            The field over which the predicate ranges
	 * @param op
	 *            The logical operation in the predicate
	 * @param constant
	 *            The value against which the field is compared
	 * @return The estimated selectivity (fraction of tuples that satisfy) the
	 *         predicate
	 */
	public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
		// some code goes here
		if (td.getFieldType(field) == Type.INT_TYPE) {
			//System.out.println(intH.get(field).toString());
			//System.out.println(((IntField)constant).getValue());
			return intH.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
		}
		else {
			return stringH.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
		}
	}

	/**
	 * return the total number of tuples in this table
	 * */
	public int totalTuples() {
		// some code goes here
		return numTuples;
	}

}
