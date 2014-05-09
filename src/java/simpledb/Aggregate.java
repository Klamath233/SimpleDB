package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

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
    
    Aggregator aggr;
    DbIterator child;
    DbIterator aggrIt;
    DbIterator[] children;
    int groupField;
    String groupFieldName;
    int aggrField;
    String aggrFieldName;
    Aggregator.Op operator;
    TupleDesc td;
    
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	this.child = child;
    	Type atype = child.getTupleDesc().getFieldType(afield);
    	Type gtype = null;
    	if (gfield > -1) {
    		gtype = child.getTupleDesc().getFieldType(gfield);
    	}
    	if (atype == Type.INT_TYPE) {
    		this.aggr = new IntegerAggregator(gfield, gtype, afield, aop);
    	} else {
    		this.aggr = new StringAggregator(gfield, gtype, afield, aop);
    	}
    	
    	this.children = new DbIterator[1];
    	this.children[0] = this.child;
    	this.groupField = gfield;
    	if (gfield > -1) {
    		this.groupFieldName = child.getTupleDesc().getFieldName(gfield);
    	}
    	this.aggrField = afield;
    	this.aggrFieldName = child.getTupleDesc().getFieldName(afield);
    	this.operator = aop;
    	this.aggrIt = this.aggr.iterator();
    	
    	if (gfield > -1) {
    		Type[] typeAr = {gtype, atype};
    		String[] NameAr = {this.groupFieldName, this.aggrFieldName};
        	this.td = new TupleDesc(typeAr, NameAr);
    	} else {
    		Type[] typeAr = {atype};
    		String[] NameAr = {this.aggrFieldName};
        	this.td = new TupleDesc(typeAr, NameAr);
    	}
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    	return this.groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
		return this.groupFieldName;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
		return this.aggrField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
		return this.aggrFieldName;
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
		return this.operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	super.open();
    	this.child.open();
    	while(this.child.hasNext()) {
    		this.aggr.mergeTupleIntoGroup(this.child.next());
    	}
    	this.aggrIt = this.aggr.iterator();
    	this.aggrIt.open();
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
    	if (this.aggrIt.hasNext()) {
    		return this.aggrIt.next();
    	} else {
    		return null;
    	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	this.aggrIt.rewind();
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
    	return this.td;
    }

    public void close() {
	// some code goes here
    	super.close();
    	this.child.close();
    	this.aggrIt.close();
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
		return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
    	this.children = children;
    }
    
}
