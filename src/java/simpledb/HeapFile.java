package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    public File file;
    public TupleDesc tupleDesc;
    public AtomicInteger numPage;

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
        this.tupleDesc = td;
        this.numPage = new AtomicInteger(((int) file.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize());
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
     * you will need to generate this tableid somewhere to ensure that each
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
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            int pos = BufferPool.getPageSize() * pid.getPageNumber();
            if (pos < randomAccessFile.length()) {
                randomAccessFile.seek(pos);
                randomAccessFile.read(data);
                randomAccessFile.close();
            }
            HeapPage page = new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
            //System.out.println("readPage"  + pid.getPageNumber());
            // Write this page back to file, in case the file is empty, we need to wirte it back
            // to update the number of Page.
            //writePage(page);
            return page;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.seek(BufferPool.getPageSize() * page.getId().getPageNumber());
            randomAccessFile.write(page.getPageData());
            randomAccessFile.close();
            //this.numPage = ((int)file.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //System.out.println("file size: "  + this.file.length());
        int res = ((int) file.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize();
        //return numPage.get();
        return Math.max(res, numPage.get());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> res = new ArrayList<Page>();
        for (int i = 0; i < this.numPage.get(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i),
                    Permissions.READ_WRITE);
            
            if (page.getNumEmptySlots() > 0 ) {
                //System.out.println("1");
                page.insertTuple(t);
                res.add(page);
                return res;
            }
        }
        // If thers is no slot empty, allocate new Page
        int pageNum = this.numPage.getAndIncrement();
        //System.out.println("Total Page:" + pageNum);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), pageNum),
        Permissions.READ_WRITE);
        page.insertTuple(t);
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> res = new ArrayList<Page>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, getId(), simpledb.Permissions.READ_ONLY);
    }

    public class HeapFileIterator implements DbFileIterator {

        public TransactionId tid;
        public int tableId;
        public simpledb.Permissions perm;
        public int pageNum;
        public Iterator<Tuple> tupleIterator;

        public HeapFileIterator(TransactionId tid, int tableId, simpledb.Permissions perm) {
            this.tid = tid;
            this.tableId = tableId;
            this.perm = perm;
            this.pageNum = 0;
            tupleIterator = null;
        }

        public void open() throws DbException, TransactionAbortedException {
            HeapPageId pageId = new HeapPageId(tableId, pageNum);
            tupleIterator = openPage(pageNum, pageId).iterator();
        }

        private HeapPage openPage(int pageNum, HeapPageId pageId) 
                    throws DbException, TransactionAbortedException {
            if (pageNum < 0 || pageNum >= numPages()) {
                return null;
                //throw new DbException("No Page");
            }
            return (HeapPage) Database.getBufferPool().getPage(tid, pageId, this.perm);
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null || pageNum >= numPages()) {
                return false;
            }
            while (!tupleIterator.hasNext() && tupleIterator != null) {
                pageNum ++;
                if (pageNum >= numPages()) {
                    return false;
                }
                HeapPageId pageId = new HeapPageId(tableId, pageNum);
                tupleIterator = openPage(pageNum, pageId).iterator();
            }
            return true;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        public void rewind() throws DbException, TransactionAbortedException{
            this.pageNum = 0;
            open();
        }

        public void close() {
            this.pageNum = 0;
            tupleIterator = null;
        }
    }

}

