package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    // Private variables.
    TransactionId t;
    DbIterator child;
    DbIterator[] children;
    boolean valid;
    int count;
    Tuple countTup;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.t = t;
    	this.child = child;
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
    	this.count = 0;
    }

    public void close() {
        // some code goes here
    	super.close();
    	this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    	this.valid = true;
    	this.count = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	while (this.child.hasNext()) {
        	try {
				Database.getBufferPool().deleteTuple(this.t, this.child.next());
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
