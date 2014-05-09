// Apr 17, 2014. Code commented by Xi Han.
package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

 	// The container for holding items.   
    private Vector<TDItem> items;
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return this.items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	
    	this.items = new Vector<TDItem>();
    	for (int i = 0; i < typeAr.length; i++) {
    		this.items.add(new TDItem(typeAr[i], fieldAr[i]));
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {

    	this.items = new Vector<TDItem>();
    	for (int i = 0; i < typeAr.length; i++) {
    		this.items.add(new TDItem(typeAr[i], null));
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	String fieldName;
        try {
        	fieldName = this.items.get(i).fieldName;
        } catch (ArrayIndexOutOfBoundsException e) {
        	throw new NoSuchElementException();
        }
        
        if (fieldName == null) {
        	return "null";
        } else {
        	return fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	Type fieldType;
        try {
        	fieldType = this.items.get(i).fieldType;
        } catch (ArrayIndexOutOfBoundsException e) {
        	System.out.println(i);
        	throw new NoSuchElementException();
        }
        return fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	for (int i = 0; i < this.items.size(); i++) {
    		if (this.items.get(i).fieldName != null &&
    				name != null &&
    				this.items.get(i).fieldName.equals(name)) {
    			return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // Calculate the size of a tuple by calling Type's getLen method.
    	int accumulator = 0;
    	for (Iterator<TDItem> it = this.items.iterator(); it.hasNext();) {
    		accumulator += it.next().fieldType.getLen();
    	}
        return accumulator;
    }
    
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	int td1Length = td1.numFields();
    	int td2Length = td2.numFields();
    	
    	Type[] typeAr = new Type[td1Length + td2Length];
    	String[] fieldAr = new String[td1Length + td2Length];
    	
    	for (int i = 0; i < td1Length; i++) {
    		typeAr[i] = td1.getFieldType(i);
    		fieldAr[i] = td1.getFieldName(i);
    	}
    	
    	for (int i = 0; i < td2Length; i++) {
    		typeAr[i + td1Length] = td2.getFieldType(i);
    		fieldAr[i + td1Length] = td2.getFieldName(i);
    	}
    	
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o instanceof TupleDesc) {
        	TupleDesc other = (TupleDesc) o;
        	if (this.items.size() == other.items.size()) {
        		for (int i = 0; i < this.items.size(); i++) {
        			if (!this.getFieldType(i).equals(other.getFieldType(i))) {
        				return false;
        			}
        		}
        		return true;
        	}
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer("");
        for (Iterator<TDItem> it = this.iterator(); it.hasNext();) {
        	TDItem item = it.next();
        	sb.append(item.fieldType.toString());
        	sb.append("(" + item.fieldName +")");
        	
        	if (it.hasNext()) {
        		sb.append(", ");
        	} else {
        		sb.append(".");
        	}
        }
        return sb.toString();
    }
}
