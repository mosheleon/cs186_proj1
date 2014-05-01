package simpledb;

import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	/**
	 * Create a new IntHistogram.
	 * 
	 * This IntHistogram should maintain a histogram of integer values that it receives.
	 * It should split the histogram into "buckets" buckets.
	 * 
	 * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
	 * 
	 * Your implementation should use space and have execution time that are both
	 * constant with respect to the number of values being histogrammed. For example, you shouldn't 
	 * simply store every value that you see in a sorted list.
	 * 
	 * @param buckets The number of buckets to split the input value into.
	 * @param min The minimum integer value that will ever be passed to this class for histogramming
	 * @param max The maximum integer value that will ever be passed to this class for histogramming
	 */

	private HashMap<Integer, Integer> H;
	private double ntups = 0;
	private double bucket_width = 0;
	private int buckets = 0;
	private int min;
	private int max;

	public IntHistogram(int buckets, int min, int max) {
		// some code goes here
		this.buckets = buckets;
		//this.bucket_width = ((double)(max - min))/((double)buckets);
		this.bucket_width = Math.max(1, ((double)(max - min))/((double)buckets));
		this.min = min;
		this.max = max;
		this.H = new HashMap<Integer, Integer>();
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		// some code goes here
		// each bucket is inclusive of left and exclusive of right boundary
		// exception is last bucket, which is inclusive of max
		int bucket_no = (int)Math.floor((v - min)/bucket_width);
		if (bucket_no == buckets)
			bucket_no = buckets-1; //make last bucket inclusive of max
		if (H.containsKey(bucket_no))
			H.put(bucket_no, H.get(bucket_no)+1);
		else
			H.put(bucket_no, 1);
		ntups++;
	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this table.
	 * 
	 * For example, if "op" is "GREATER_THAN" and "v" is 5, 
	 * return your estimate of the fraction of elements that are greater than 5.
	 * 
	 * @param op Operator
	 * @param v Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {
		int bucket_no = (int)Math.floor((v - min)/bucket_width);
		if (bucket_no == buckets)
			bucket_no = buckets-1; //make last bucket inclusive of max

		double sel = 0;
		int i = 0;
		double b_f = 0;
		double b_right = 0;
		double b_part = 0;
		double b_sel = 0;
		double b_left = 0;
		double b_h = 0;
		
		switch(op) {
		case EQUALS:
			if (bucket_no < 0)
				return 0.0;
			if (bucket_no > buckets-1)
				return 0.0;
			if (H.containsKey(bucket_no))
				return (H.get(bucket_no)/bucket_width)/ntups;
			else
				return 0.0;

		case GREATER_THAN:
			if (bucket_no < 0)
				return 1.0;
			if (bucket_no > buckets-1)
				return 0.0;
			sel = 0.0;
			i = 0;
			if (H.containsKey(bucket_no))
				b_f = H.get(bucket_no)/ntups;
			else
				b_f = 0.0;
			b_right = min + (bucket_no+1)*bucket_width;
			b_part = (b_right-v)/bucket_width;
			b_sel = b_f * b_part;
			sel += b_sel;

			for (i = bucket_no+1; i < buckets; i++) {
				if (H.containsKey(i))
					sel += H.get(i)/ntups;
			}
			return sel;

		case LESS_THAN:
			if (bucket_no < 0)
				return 0;
			if (bucket_no > buckets-1)
				return 1.0;
			sel = 0.0;
			i = 0;
			if (H.containsKey(bucket_no))
				b_f = H.get(bucket_no)/ntups;
			else
				b_f = 0.0;
			b_left = min + bucket_no*bucket_width;
			b_part = (v-b_left)/bucket_width;
			b_sel = b_f * b_part;
			sel += b_sel;

			for (i = bucket_no-1; i >= 0; i--) {
				if (H.containsKey(i))
					sel += H.get(i)/ntups;
			}
			return sel;

		case LESS_THAN_OR_EQ:
			if (bucket_no < 0)
				return 0;
			if (bucket_no >= buckets-1)
				return 1.0;
			sel = 0.0;
			if (H.containsKey(bucket_no))
				sel += (H.get(bucket_no)/bucket_width)/ntups;
			i = 0;
			if (H.containsKey(bucket_no))
				b_f = H.get(bucket_no)/ntups;
			else
				b_f = 0.0;
			b_left = min + bucket_no*bucket_width;
			b_part = (v-b_left)/bucket_width;
			b_sel = b_f * b_part;
			sel += b_sel;

			for (i = bucket_no-1; i >= 0; i--) {
				if (H.containsKey(i))
					sel += H.get(i)/ntups;
			}
			return sel;
			
		case GREATER_THAN_OR_EQ:
			if (bucket_no <= 0)
				return 1.0;
			if (bucket_no > buckets-1)
				return 0;

			sel = 0.0;
			if (H.containsKey(bucket_no))
				sel += (H.get(bucket_no)/bucket_width)/ntups;
			i = 0;
			if (H.containsKey(bucket_no))
				b_f = H.get(bucket_no)/ntups;
			else
				b_f = 0.0;
			b_right = min + (bucket_no+1)*bucket_width;
			b_part = (b_right-v)/bucket_width;
			b_sel = b_f * b_part;
			sel += b_sel;

			for (i = bucket_no+1; i < buckets; i++) {
				if (H.containsKey(i))
					sel += H.get(i)/ntups;
			}
			return sel;
			
		case NOT_EQUALS:
			if (bucket_no < 0)
				return 1.0;
			if (bucket_no > buckets-1)
				return 1.0;
			if (H.containsKey(bucket_no))
				return 1.0 - (H.get(bucket_no)/bucket_width)/ntups;
			else
				return 1.0;
		}
		// some code goes here
		return -1.0;
	}

	/**
	 * @return
	 *     the average selectivity of this histogram.
	 *     
	 *     This is not an indispensable method to implement the basic
	 *     join optimization. It may be needed if you want to
	 *     implement a more efficient optimization
	 * */
	public double avgSelectivity()
	{
		// some code goes here
		return 1.0;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {

		// some code goes here
		String result = "Num Tuples : " + this.ntups + " bucket width : " + this.bucket_width + " num buckets : " + this.buckets + "\n";
		for (int i = 0; i < buckets; i ++ ) {
			double b_left = min + i*bucket_width;
			double b_right = min + (i+1)*bucket_width;
			result += "[" + b_left + "," + b_right + "] : " + H.get(i) + "\n";
		}
		return result;
	}
}
