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

	private File file;
	private TupleDesc td;
	private int maxPageNo;
	
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
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
        // some code goes here
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
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int pageNo = pid.pageNumber();
    	int offset = BufferPool.getPageSize() * pageNo;
    	byte[] data = new byte[BufferPool.getPageSize()];
    	RandomAccessFile raf = null;
    	try {
			raf = new RandomAccessFile(this.file, "r");
			raf.seek((long) offset); 
			raf.read(data, 0, BufferPool.getPageSize());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        try {
			return new HeapPage((HeapPageId) pid, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
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
        // some code goes here
        return new DbFileIterator() {

        	private int currentPageNo = 0;
        	private Page currentPage = null;
        	private PageId currentPageId = null;
        	private Iterator<Tuple> tuples = null;
        	private TransactionId tid = null;
        	
        	private DbFileIterator setTid(TransactionId tid) {
        		this.tid = tid;
        		return this;
        	}
        	
			@Override
			public void open() throws DbException, TransactionAbortedException {
				int tableId = HeapFile.this.getId();
				this.currentPageId = new HeapPageId(tableId, this.currentPageNo);
				this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
				this.tuples = ((HeapPage) this.currentPage).iterator();
			}

			@Override
			public boolean hasNext() throws DbException,
					TransactionAbortedException {
				// TODO Auto-generated method stub
				if (this.tuples != null) {
					if (this.tuples.hasNext()) {
						return true;
					} else {
						if (this.currentPageNo < HeapFile.this.maxPageNo) {
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
				// TODO Auto-generated method stub
				if (this.tuples != null) {
					return this.tuples.next();
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				// TODO Auto-generated method stub
				int tableId = HeapFile.this.getId();
				this.currentPageNo = 0;
				this.currentPageId = new HeapPageId(tableId, this.currentPageNo);
				this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
				this.tuples = ((HeapPage) this.currentPage).iterator();
			}

			@Override
			public void close() {
				// TODO Auto-generated method stub
				this.currentPageNo = 0;
				this.currentPageId = null;
				this.currentPage = null;
				this.tuples = null;
				
			}
        	
        }.setTid(tid);
    }

}

