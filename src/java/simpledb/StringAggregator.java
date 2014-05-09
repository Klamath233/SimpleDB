package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Group;
import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;



    // Private Variables
    int gbField;
    Type gbFieldType; // INT_TYPE or null. Arity of the aggragate relation will depend on this.
    int aggrField;
    int tupNum;
    Op aggrOperator;
    HashMap<Field, Group> groups;
    Group nogroup;
    String groupName;
    
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
    	if (what != Op.COUNT) {
    		throw new IllegalArgumentException("StringAggregator: operator not equals COUNT");
    	}
    	this.gbField = gbfield;
    	this.gbFieldType = gbfieldtype;
    	this.aggrField = afield;
    	this.aggrOperator = what;
    	this.groups = new HashMap<Field, Group>();
    	this.nogroup = new Group(null);
    	this.groupName = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
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
    		if (this.groups.containsKey(tup.getField(this.gbField))) {
    			this.groups.get(tup.getField(this.gbField)).tuples.add(tup);
    		} else {
    			Group newGroup = new Group(tup.getField(this.gbField));
    			newGroup.tuples.add(tup);
    			this.groups.put(tup.getField(this.gbField), newGroup);
    		}
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
        return new DbIterator() {
        	
        	Iterator<Group> groupIt;
        	TupleDesc td;
        	boolean hasNoGroupBeenAccessed;
        	
			@Override
			public void open() throws DbException, TransactionAbortedException {
				this.hasNoGroupBeenAccessed = false;
				this.groupIt = groups.values().iterator();				
				if (!groupIt.hasNext()) {
					throw new DbException("IntegerAggregator: open failed");
				}
				
				if (groupName != null) {
					Type[] typeAr = {gbFieldType, Type.INT_TYPE};
					String[] nameAr = {groupName, aggrOperator.toString()};
					td = new TupleDesc(typeAr, nameAr);
				} else {
					Type[] typeAr = {gbFieldType};
					String[] nameAr = {aggrOperator.toString()};
					td = new TupleDesc(typeAr, nameAr);
				}
			}

			@Override
			public boolean hasNext() throws DbException,
					TransactionAbortedException {
				if (gbField != Aggregator.NO_GROUPING) {
					return this.groupIt.hasNext();
				} else {
					return !this.hasNoGroupBeenAccessed;
				}
			}

			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				// TODO Auto-generated method stub
				
				Group currentGroup = null;
				
				if (gbField != Aggregator.NO_GROUPING) {
					currentGroup = this.groupIt.next();
				} else {
					currentGroup = nogroup;
					this.hasNoGroupBeenAccessed = true;
				}
				
				int acc = 0;
				
				if (aggrOperator == Op.COUNT) {
					for (Iterator<Tuple> it = currentGroup.tuples.iterator(); it.hasNext();) {
						it.next();
						acc++;
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
				this.hasNoGroupBeenAccessed = false;
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
				this.hasNoGroupBeenAccessed = true;
			}
        };
    }

}
