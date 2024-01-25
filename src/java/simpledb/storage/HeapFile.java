package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    private File f;
    private TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here; MAY DONE
        return f;
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
        // TODO: some code goes here; MAY DONE
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here; MAY DONE
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here; MAY DONE
        int pageSize = BufferPool.getPageSize();
        int pageOffset = pid.getPageNumber() * pageSize;

        try {
            FileInputStream fis = new FileInputStream(f);
            byte[] bytes = new byte[pageSize];
            fis.read(bytes, pageOffset, pageSize);

            HeapPageId hPid = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(hPid, bytes);
        } catch (IOException e) {
            // this really shouldn't happen
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here; MAY DONE
        return (int) f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator {
        private HeapFile heapF;
        private TupleDesc td;
        private FileInputStream fis;
        private int page_no = 0;
        private Iterator<Tuple> it;
        private TransactionId tid;
        private boolean opened;

        public HeapFileIterator(HeapFile heapF, TransactionId tid) {
            this.heapF = heapF;
            this.tid = tid;

        }

        private void readPage() {
            HeapPageId pid = new HeapPageId(heapF.getId(), page_no++); 
            HeapPage f = (HeapPage) heapF.readPage(pid);
            it = f.iterator();
        }

        public void open() {
            opened = true;
            page_no = 0;
            readPage();
        }

        public boolean hasNext() {
            if (! opened) {
                return false;
            }
            if (heapF.numPages() > page_no + 1) {
                return true;
            }
            return it.hasNext();
        }

        public Tuple next() {
            if (! opened) {
                throw new NoSuchElementException();
            }

            if (it.hasNext()) {
                return it.next();
            }
            readPage();

            return it.next();

        }

        public void rewind() {
            page_no = 0;
            readPage();
        }
        
        public void close() {
            page_no = 0;
            opened = false;
        }
    }

}

