package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Vector;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private class IntGroup {
    	IntField gbField;
    	Vector<Tuple> tuples;
    	
    	public IntGroup(IntField gbField) {
    		this.gbField = gbField;
    		this.tuples = new Vector<Tuple>();
    	}
    }
    
    // Private Variables
    int gbField;
    Type gbFieldType; // INT_TYPE or null. Arity of the aggragate relation will depend on this.
    int aggrField;
    int tupNum;
    Op aggrOperator;
    HashMap<IntField, IntGroup> groups;
    IntGroup nogroup;
    String groupName;
    
    // If no group, the groups vector will have only one tuple.
    // Otherwise, it will be filled with the groups.
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbField = gbfield;
    	this.gbFieldType = gbfieldtype;
    	this.aggrField = afield;
    	this.aggrOperator = what;
    	this.groups = new HashMap<IntField, IntGroup>();
    	this.nogroup = new IntGroup(null);
    	this.groupName = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (groupName == null && this.gbFieldType != null) {
    		groupName = tup.getTupleDesc().getFieldName(this.gbField);
    		if (groupName == null) {
    			groupName = "null";
    		}
    	}
    	
    	if (this.gbField == Aggregator.NO_GROUPING || this.gbFieldType == null) {
    		// Case 1: no grouping.
    		// Add the tuple to the only group ==> this.nogroup.
    		this.nogroup.tuples.add(tup);
    	} else {
    		// Case 2: grouping.
    		// If the group for gbField exists, add the tuple into that group.
    		// Otherwise, create a group and add the tuple.
    		if (this.groups.containsKey((IntField) tup.getField(this.gbField))) {
    			this.groups.get((IntField) tup.getField(this.gbField)).tuples.add(tup);
    		} else {
    			IntGroup newGroup = new IntGroup((IntField) tup.getField(this.gbField));
    			newGroup.tuples.add(tup);
    			this.groups.put((IntField) tup.getField(this.gbField), newGroup);
    		}
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new DbIterator() {
        	
        	Iterator<IntGroup> groupIt;
        	TupleDesc td;
        	
			@Override
			public void open() throws DbException, TransactionAbortedException {
				// TODO Auto-generated method stub
				this.groupIt = groups.values().iterator();				
				if (!groupIt.hasNext()) {
					throw new DbException("IntegerAggregator: open failed");
				}
				
				if (groupName != null) {
					Type[] typeAr = {Type.INT_TYPE, Type.INT_TYPE};
					String[] nameAr = {groupName, aggrOperator.toString()};
					td = new TupleDesc(typeAr, nameAr);
				} else {
					Type[] typeAr = {Type.INT_TYPE};
					String[] nameAr = {aggrOperator.toString()};
					td = new TupleDesc(typeAr, nameAr);
				}
			}

			@Override
			public boolean hasNext() throws DbException,
					TransactionAbortedException {
				// TODO Auto-generated method stub
				return this.groupIt.hasNext();
			}

			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				// TODO Auto-generated method stub
				IntGroup currentGroup = this.groupIt.next();
				int acc = 0;
				
				if (aggrOperator == Op.AVG) {
					for (Iterator<Tuple> it = currentGroup.tuples.iterator(); it.hasNext();) {
						acc += ((IntField) it.next().getField(aggrField)).getValue();
					}
					acc /= currentGroup.tuples.size();
				}
				
				if (aggrOperator == Op.COUNT) {
					for (Iterator<Tuple> it = currentGroup.tuples.iterator(); it.hasNext();) {
						acc++;
					}
				}
				
				if (aggrOperator == Op.SUM) {
					for (Iterator<Tuple> it = currentGroup.tuples.iterator(); it.hasNext();) {
						acc += ((IntField) it.next().getField(aggrField)).getValue();
					}
				}
				
				if (aggrOperator == Op.MAX) {
					acc = Integer.MIN_VALUE;
					for (Iterator<Tuple> it = currentGroup.tuples.iterator(); it.hasNext();) {
						int next = ((IntField) it.next().getField(aggrField)).getValue();
						if (next > acc) {
							acc = next;
						}
					}
				}
				
				if (aggrOperator == Op.MIN) {
					acc = Integer.MAX_VALUE;
					for (Iterator<Tuple> it = currentGroup.tuples.iterator(); it.hasNext();) {
						int next = ((IntField) it.next().getField(aggrField)).getValue();
						if (next < acc) {
							acc = next;
						}
					}
				}
				
				Tuple rettup = new Tuple(this.td);
				if (this.td.numFields() == 1) {
					rettup.setField(0, new IntField(acc));
				} else {
					rettup.setField(0, currentGroup.gbField);
					rettup.setField(1, new IntField(acc));
				}
				return rettup;
			}

			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				this.groupIt = groups.values().iterator();
			}

			@Override
			public TupleDesc getTupleDesc() {
				// TODO Auto-generated method stub
				return this.td;
			}

			@Override
			public void close() {
				// TODO Auto-generated method stub
				this.groupIt = null;
			}
        };
    }

}
