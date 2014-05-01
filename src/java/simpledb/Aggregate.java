package simpledb;

import java.util.*;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

	private static final long serialVersionUID = 1L;
	private DbIterator child;
	private int afield;
	private int gfield;
	private Aggregator.Op aop;
	private Aggregator agg;
	private DbIterator itr;

	/**
	 * Constructor.
	 * 
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntAggregator} or {@link StringAggregator} to help
	 * you with your implementation of readNext().
	 * 
	 * 
	 * @param child
	 *            The DbIterator that is feeding us tuples.
	 * @param afield
	 *            The column over which we are computing an aggregate.
	 * @param gfield
	 *            The column over which we are grouping the result, or -1 if
	 *            there is no grouping
	 * @param aop
	 *            The aggregation operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here
		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;
		this.itr = null;
		TupleDesc td = child.getTupleDesc();
		Type gfieldtype;
		if (gfield != Aggregator.NO_GROUPING)
			gfieldtype = td.getFieldType(gfield);
		else
			gfieldtype = null;
		if (td.getFieldType(afield) == Type.INT_TYPE)
			this.agg = new IntegerAggregator(gfield, gfieldtype, afield, aop);
		else 
			this.agg = new StringAggregator(gfield, gfieldtype, afield, aop);
	}

	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 *         field index in the <b>INPUT</b> tuples. If not, return
	 *         {@link simpledb.Aggregator#NO_GROUPING}
	 **/
	public int groupField() {
		// some code goes here
		return this.gfield;
	}

	/**
	 * @return If this aggregate is accompanied by a group by, return the name
	 *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
	 *         null;
	 * */
	public String groupFieldName() {
		// some code goes here
		TupleDesc td = getTupleDesc();
		if (gfield != Aggregator.NO_GROUPING)
			return td.getFieldName(0);
		else
			return null;
	}

	/**
	 * @return the aggregate field
	 * */
	public int aggregateField() {
		// some code goes here
		return this.afield;
	}

	/**
	 * @return return the name of the aggregate field in the <b>OUTPUT</b>
	 *         tuples
	 * */
	public String aggregateFieldName() {
		// some code goes here
		TupleDesc td = getTupleDesc();
		if (gfield != Aggregator.NO_GROUPING)
			return td.getFieldName(1);
		else
			return td.getFieldName(0);
	}

	/**
	 * @return return the aggregate operator
	 * */
	public Aggregator.Op aggregateOp() {
		// some code goes here
		return this.aop;
	}

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
	}

	public void open() throws NoSuchElementException, DbException,
	TransactionAbortedException {
		// some code goes here
		super.open();
		child.open();
		if (this.itr == null) {
			while(child.hasNext()){
				agg.mergeTupleIntoGroup(child.next());
			}
		}
		itr = agg.iterator();
		itr.open();
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate, If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (itr.hasNext())
			return itr.next();
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child.rewind();
		itr.rewind();
		super.close();
		super.open();
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		TupleDesc child_td = child.getTupleDesc();

		if (gfield != Aggregator.NO_GROUPING) {
			String gbfieldname = child_td.getFieldName(gfield);
			String afieldname = "(" + aop.toString() + ") ";
			String old_afieldname = child_td.getFieldName(afield);
			if (old_afieldname != null) 
				afieldname += old_afieldname;
			Type[] typeArray;
			String[] fieldNameArray;
			typeArray = new Type[2];
			typeArray[0] = child_td.getFieldType(gfield);
			typeArray[1] = Type.INT_TYPE;
			fieldNameArray = new String[2];
			fieldNameArray[0] = gbfieldname;
			fieldNameArray[1] = afieldname;
			return new TupleDesc(typeArray, fieldNameArray);

		}
		else {
			String afieldname = "(" + aop.toString() + ") ";
			String old_afieldname = child_td.getFieldName(afield);
			if (old_afieldname != null) 
				afieldname += old_afieldname;
			Type[] typeArray;
			String[] fieldNameArray;
			typeArray = new Type[1];
			typeArray[0] = Type.INT_TYPE;
			fieldNameArray = new String[1];
			fieldNameArray[0] = afieldname;
			return new TupleDesc(typeArray, fieldNameArray);
		}
	}

	public void close() {
		// some code goes here
		child.close();
		super.close();
		itr.close();
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
