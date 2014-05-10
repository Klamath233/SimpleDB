// Apr 17, 2014. Code commented by Xi Han.
package simpledb;

import java.io.*;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE; // Page size used by the pool.
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    // Comments needed.
    private ConcurrentHashMap<PageId, Page> pool; // The container of the pages.
    private int maxSize; // The max page number of the pool.

    private ConcurrentHashMap<PageId, Integer> lruCache;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
		// Instantiate instance variables.
    	this.maxSize = numPages;
    	this.pool = new ConcurrentHashMap<PageId, Page>();
        this.lruCache = new ConcurrentHashMap<PageId,Integer>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // First, try to get the page from the buffer pool.
    	Page page = this.pool.get(pid);
    	if (page != null) {
            // Do LRU Cache update
            for(PageId piddd: this.lruCache.keySet())
            {
                int times = this.lruCache.get(piddd); 
                this.lruCache.replace(piddd,times+1);
            }
            this.lruCache.replace(pid,1);
			// On success, return the page.
    		return page;
    	} else {
			// On failure, read the file from the disk.
    		page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    		if (this.pool.size() == this.maxSize) {
                this.evictPage(); // Do eviction
                //System.err.println(this.pool.size()+1 == this.maxSize);
                //System.err.println(this.maxSize);
    		}

			// Put the page into the buffer.
            this.lruCache.put(pid,1);
    		this.pool.put(pid, page);
    		return page;
    	}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    public Iterator<PageId> getPidIterator() {
    	return this.pool.keySet().iterator();
    }
    
    public Iterator<Page> getPageIterator() {
    	return this.pool.values().iterator();
    }
    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile table = Database.getCatalog().getDatabaseFile(tableId);
    	table.insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	((HeapPage) getPage(tid, t.getRecordId().getPageId(), null)).deleteTuple(t);    	
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // flush all dirty pages to disk
        // we can simply use for loop and flush clean page as well
        for(PageId pid:this.pool.keySet())
        {
            Page p = this.pool.get(pid);
            if (p.isDirty() != null)
            {
                this.flushPage(pid);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        DbFile df = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page to_be_written = this.pool.get(pid);
        // if no matching page in hashmap, we do not need to flush
        if ( to_be_written == null)
        {
            return;
        }
        else
        {
            df.writePage(to_be_written); // write
            to_be_written.markDirty(false,null); // mark clean

        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId temppid = null;
        int tempmax = 1;
        for(PageId pid: this.lruCache.keySet())
        {
            int v = this.lruCache.get(pid);
            if(v >= tempmax)
            {
                temppid = pid;
                tempmax = v;
            }
        }
        if (temppid == null)
        {
            throw new DbException("Should exist victim page!\n");
        }
        else
        {
            for(PageId pid: this.lruCache.keySet())
            {
                int times = this.lruCache.get(pid); 
                this.lruCache.replace(pid,times+1);
            }
            this.lruCache.remove(temppid);
            this.pool.remove(temppid);
        }
    }

}
