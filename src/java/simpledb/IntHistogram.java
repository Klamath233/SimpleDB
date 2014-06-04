package simpledb;

import java.util.Vector;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private class Bucket {
		public int b_min;
		public int b_max;
		public int b_height;
		public int b_width;
		
		public Bucket(int min, int max) {
			this.b_min = min;
			this.b_max = max;
			this.b_height = 0;
			this.b_width = max - min + 1;
		}
		
	}
	
	private int numBuckets;
	private int count;
	private int min;
	private int max;
	private int floorWidth;
	private int ceilingWidth;
	private int firstCeiling;
	private Vector<Bucket> buckets;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.min = min;
    	this.max = max;
    	this.numBuckets = buckets;
    	this.count = 0;
    	int numBucs = this.numBuckets;
    	int numNums = this.max - this.min + 1;
    	this.buckets = new Vector<Bucket>();
    	if (numBucs > numNums) {
    		this.numBuckets = numNums;
    		numBucs = numNums;
    	}
    	int cursor = this.min;
    	this.floorWidth = (int) Math.floor((double) (numNums / numBucs));
    	this.ceilingWidth = (int) Math.ceil((double) (numNums / numBucs));
    	this.firstCeiling = 0;
    	boolean completeDetectingFC = false;
    	
    	// Make buckets size averaged.
    	while (numBucs > 0) {
    		if (!completeDetectingFC && numNums % numBucs == 0) {
    			this.firstCeiling = this.buckets.size();
    			completeDetectingFC = true;
    		}
    		
    		int width = numNums / numBucs;
    		numNums -= width;
    		numBucs--;
    		
    		int b_min = cursor;
    		int b_max = cursor + width - 1;
    		this.buckets.add(new Bucket(b_min, b_max));
    		cursor += width;
    	}
    	assert(cursor == this.max + 1);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	int idx = findBucket(v);
    	this.buckets.get(idx).b_height++;
    	this.count++;
    }
    
    private int findBucket(int v) {
    	int offset = v - this.min;
    	if (offset < this.floorWidth * this.firstCeiling) {
    		return offset / this.floorWidth;
    	} else {
    		return this.firstCeiling + (offset - this.firstCeiling * this.firstCeiling) / this.ceilingWidth;
    	}
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

    	// some code goes here
    	if (this.count == 0) {
    		return Double.MAX_VALUE;
    	}
    	
    	double ntups = this.count;
    	
    	if (op.equals(Predicate.Op.EQUALS)) {
    		return estimateEqualSelectivity(v, ntups);
    	} else if (op.equals(Predicate.Op.NOT_EQUALS)) {
    		return 1.0 - estimateEqualSelectivity(v, ntups);
    	} else if (op.equals(Predicate.Op.GREATER_THAN)) {
    		return estimateGreaterThanSelectivity(v, ntups);
    	} else if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
    		return estimateEqualSelectivity(v, ntups) + estimateGreaterThanSelectivity(v, ntups);
    	} else if (op.equals(Predicate.Op.LESS_THAN)) {
    		return (1.0 - (estimateEqualSelectivity(v, ntups) + estimateGreaterThanSelectivity(v, ntups)));
    	} else if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
    		return (1.0 - estimateGreaterThanSelectivity(v, ntups));
    	}
    	
        return -1.0;
    }
    
    private double estimateEqualSelectivity(int v, double ntups) {
    	
    	// If v is out of range, return 0.0.
    	if (v < this.min || v > this.max) {
    		return 0.0;
    	}
    	
    	// Get the bucket
    	Bucket b = this.buckets.get(findBucket(v));
    	// Calculate the selectivity.
    	double h = b.b_height;
    	double w = b.b_width;
    	return (h / w) / ntups;
    }
    
    private double estimateGreaterThanSelectivity(int v, double ntups) {
    	
    	// Similar to previous function.
    	if (v < this.min) {
    		return 1.0;
    	}
    	
    	if (v >= this.max) {
    		return 0.0;
    	}
    	
    	// Get the index of the bucket.
    	Bucket b = null;
    	int idx = findBucket(v);
    	for (int i = 0; i < this.numBuckets; i++) {
    		b = this.buckets.get(i);
    		if (v >= b.b_min && v <= b.b_max) {
    			idx = i;
    			break;
    		}
    	}
    	
    	double acc = 0.0;
    	double h = b.b_height;
    	double w = b.b_width;
    	acc += (this.buckets.get(idx).b_max - v) / w * (h / ntups);
    	
    	idx++;
    	while (idx < this.numBuckets) {
    		b = this.buckets.get(idx);
    		h = b.b_height;
    		w = b.b_width;
    		acc += h / ntups;
    		idx++;
    	}
    	return acc;
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
        return null;
    }
}
