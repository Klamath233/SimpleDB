package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    // Private variables.
    TransactionId t;
    DbIterator child;
    int tid;
    DbIterator[] children;
    boolean valid;
    int count;
    Tuple countTup;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	this.t = t;
    	this.child = child;
    	this.tid = tableid;
    	this.children = new DbIterator[1];
    	this.children[0] = child;
    	this.valid = true;
    	this.count = 0;
    	Type[] typeAr = {Type.INT_TYPE};
    	String[] stringAr = {"inserted_count"};
    	this.countTup = new Tuple(new TupleDesc(typeAr, stringAr));
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.countTup.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	this.child.open();
    }

    public void close() {
        // some code goes here
    	this.child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    	this.valid = true;
    	this.count = 0;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (this.child.hasNext()) {
        	try {
				Database.getBufferPool().insertTuple(this.t, this.tid, this.child.next());
				this.count++;
			} catch (NoSuchElementException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        }
        
        if (this.valid == true) {
        	this.countTup.setField(0, new IntField(this.count));
        	this.valid = false;
        	return this.countTup;
        } else {
        	return null;
        }
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
