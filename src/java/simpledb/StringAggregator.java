package simpledb;
import java.util.*;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private ArrayList<TupleAndCount> myGroupTuples; //Tuple of GroupVal and AggregateVal
	private TupleAndCount myGroup;
	private TupleDesc td;

	public class TupleAndCount {
		private Tuple t; // (GroupVal, AggregateVal) or (AggregateVal)
		private int count;
		private int gbfield;

		public TupleAndCount(Tuple t, int gbfield) {
			this.t = t;
			this.count = 1;
			this.gbfield = gbfield;
		}

		public Tuple getTuple() {
			return this.t;
		}

		public Field getGroupField() {
			if (gbfield != NO_GROUPING)
				return t.getField(0);
			return null;
		}

		public Field getAggregateField() {
			if (gbfield != NO_GROUPING)
				return t.getField(1);
			return t.getField(0);
		}

		public void setGroupField(Field f) {
			if (gbfield != NO_GROUPING)
				t.setField(0, f);
		}

		public void setAggregateField(Field f) {
			if (gbfield != NO_GROUPING)
				t.setField(1, f);
			else
				t.setField(0, f);
		}

		public int getCount() {
			return this.count;
		}

		public void incrementCount() {
			this.count++;
		}

	}
	/**
	 * Aggregate constructor
	 * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
	 * @param afield the 0-based index of the aggregate field in the tuple
	 * @param what aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		this.myGroupTuples = new ArrayList<TupleAndCount>();
		this.myGroup = null;
		this.td = null;
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here
//		System.out.println("tup : "+ tup.toString());
		try {				
			if (this.gbfield != NO_GROUPING) {
				myGroup = null;
				for (int i = 0; i < myGroupTuples.size(); i++) { // find tup's group
					if (myGroupTuples.get(i).getGroupField().equals(tup.getField(this.gbfield)) ) {
						myGroup = myGroupTuples.get(i);
					}
				}
			}

			// if no grouping and first merge, or if there is grouping and group was not found, 
			// myGroup is created from tup and added to list
			if (myGroup == null) {

				if (this.gbfield != NO_GROUPING) {
					if (this.td == null) {
						String gbfieldname = tup.getTupleDesc().getFieldName(this.gbfield);
						String afieldname = tup.getTupleDesc().getFieldName(this.afield);
						Type[] typeArray;
						String[] fieldNameArray;
						typeArray = new Type[2];
						typeArray[0] = gbfieldtype;
						typeArray[1] = Type.INT_TYPE;
						fieldNameArray = new String[2];
						fieldNameArray[0] = gbfieldname;
						fieldNameArray[1] = afieldname;
						this.td = new TupleDesc(typeArray, fieldNameArray);
					}
					myGroup = new TupleAndCount(new Tuple(td), gbfield);
					myGroup.setGroupField(tup.getField(this.gbfield));
				}
				else {
					if (this.td == null) {
						String afieldname = tup.getTupleDesc().getFieldName(this.afield);
						Type[] typeArray;
						String[] fieldNameArray;
						typeArray = new Type[1];
						typeArray[0] = Type.INT_TYPE;
						fieldNameArray = new String[1];
						fieldNameArray[0] = afieldname;
						this.td = new TupleDesc(typeArray, fieldNameArray);
					}
					myGroup = new TupleAndCount(new Tuple(td), gbfield);
				}
				if (what == Op.COUNT)
					myGroup.setAggregateField(new IntField(1));
				else
					myGroup.setAggregateField(tup.getField(this.afield));
				myGroupTuples.add(myGroup);
				return; 
			}

			//perform aggregate after first tup
			IntField f;

			if (what == Op.COUNT) {
				myGroup.incrementCount();
				f = new IntField( myGroup.getCount() );
				myGroup.setAggregateField(f);
//				System.out.println(myGroup.getTuple().toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("couldn't merge tuple");
		}
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal,
	 *   aggregateVal) if using group, or a single (aggregateVal) if no
	 *   grouping. The aggregateVal is determined by the type of
	 *   aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		ArrayList<Tuple> myTuples = new ArrayList<Tuple>();
		for (int i = 0; i < myGroupTuples.size(); i++) {
			myTuples.add(myGroupTuples.get(i).getTuple());
		}
		return new TupleIterator(td, myTuples);
	}

}
