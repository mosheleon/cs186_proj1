package simpledb;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         * 
         * @param s
         *            a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
        	//reaches out to the available enum elements
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "like";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }
    }
    
  //field number of passed in tuples to compare against
    private int field;
    //operation to use for comparison
    private Op op;
    //field value to compare passed in tuples to
    private Field operand;
    
    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
    	this.field = field;
    	this.op = op;
    	this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        // some code goes here
        return this.field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // some code goes here
        return this.op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // some code goes here
        return this.operand;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // some code goes here
    	//using field's compare method, compare(op, value)
    	boolean toReturn = t.getField(field).compare(this.getOp(), this.getOperand());
        return toReturn;
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string
     */
    public String toString() {
        // some code goes here
    	String toReturn = "";
    	toReturn += "Field: " + this.getField() + ", Op: " + this.getOp() + ", Operand: " + this.getOperand();
        return "";
    }
}
