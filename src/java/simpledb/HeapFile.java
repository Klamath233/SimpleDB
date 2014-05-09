// Apr. 17, 2014. Code commented by Xi Han.
package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File file; // The related file.
	private TupleDesc td; // The related tuple descriptor.
	private int maxPageNo; // The upper limit of the page number.
	
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
		// Initialize the instance variables.
    	this.file = f;
    	this.td = td;
    	this.maxPageNo = (int) f.length() / BufferPool.getPageSize() - 1;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
		// Calculate the offset of the file.
    	int pageNo = pid.pageNumber();
    	int offset = BufferPool.getPageSize() * pageNo;
    	byte[] data = new byte[BufferPool.getPageSize()]; // Set up the buffer.
    	RandomAccessFile raf = null;
    	try {
			raf = new RandomAccessFile(this.file, "r");
			// Move the file pointer to the offset we calculated.
			raf.seek((long) offset); 
			raf.read(data, 0, BufferPool.getPageSize());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();	
		}
        try {
			return new HeapPage((HeapPageId) pid, data);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				raf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pagenumber = page.getId().pageNumber();
        int offset = pagenumber * BufferPool.PAGE_SIZE;
        byte[] data = page.getPageData();
        try{
            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
            raf.seek(offset);
            raf.write(data);
            raf.close();
        }
        catch  (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        // Runhang: Notice, we should not catch IOException!
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
		// Implement the iterator as an anonymous class to avoid additional files.
        return new DbFileIterator() {

			// Variables to keep the state of the iterator.
        	private int currentPageNo = 0;
        	private Page currentPage = null;
        	private PageId currentPageId = null;
        	private Iterator<Tuple> tuples = null;
        	private TransactionId tid = null;
        	
			// Method for setting the transaction id of the iterator.
        	private DbFileIterator setTid(TransactionId tid) {
        		this.tid = tid;
        		return this;
        	}
        	
			@Override
			public void open() throws DbException, TransactionAbortedException {
				// Initialize the state.
				int tableId = HeapFile.this.getId();
				this.currentPageId = new HeapPageId(tableId, this.currentPageNo);
				this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
				this.tuples = ((HeapPage) this.currentPage).iterator();
			}

			@Override
			public boolean hasNext() throws DbException,
					TransactionAbortedException {
				if (this.tuples != null) {
					// If the tuple we hold is null, then the iterotor is closed.
					// If not, keep going.
					if (this.tuples.hasNext()) {
						// If tuples has more items, return true.
						return true;
					} else {
						if (this.currentPageNo < HeapFile.this.maxPageNo) {
							// If current held tuple is full, check if current page is the last page.
							// If not, move to next page and recursively call this function.
							int tableId = HeapFile.this.getId();
							this.currentPageNo++;
							this.currentPageId = new HeapPageId(tableId, this.currentPageNo);
							this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
							this.tuples = ((HeapPage) this.currentPage).iterator();
							return this.hasNext();
						}
					}
				}
				return false;
			}

			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				if (this.tuples != null) {
					return this.tuples.next();
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				// Simply reset our state.
				int tableId = HeapFile.this.getId();
				this.currentPageNo = 0;
				this.currentPageId = new HeapPageId(tableId, this.currentPageNo);
				this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
				this.tuples = ((HeapPage) this.currentPage).iterator();
			}

			@Override
			public void close() {
				// Invalidate the state variables.
				this.currentPageNo = 0;
				this.currentPageId = null;
				this.currentPage = null;
				this.tuples = null;
				
			}
        	
        }.setTid(tid);
    }

}

