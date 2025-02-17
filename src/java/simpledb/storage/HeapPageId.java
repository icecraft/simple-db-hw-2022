package simpledb.storage;

import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */

    private int _tableId;
    private int _pgNo;

    public HeapPageId(int tableId, int pgNo) {
        // TODO: some code goes here; MAY DONE!
        _tableId = tableId;
        _pgNo = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        // TODO: some code goes here; MAY DONE!
        return _tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *         this PageId
     */
    public int getPageNumber() {
        // TODO: some code goes here; MAY DONE
        return _pgNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *         the table number and the page number (needed if a PageId is used as a
     *         key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // TODO: some code goes here; MAY DONE

        // throw new UnsupportedOperationException("implement this");
        return getTableId() * 10000 + getPageNumber();  // no reason !!
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *         ids are the same)
     */
    public boolean equals(Object o) {
        // TODO: some code goes here; MAY DONE
        if (o == this)  {
            return true;
        }
        if  (!(o instanceof HeapPageId)) {
            return false;
        }
 
        HeapPageId oo = (HeapPageId) o;
 
        if ((oo.getTableId() != getTableId()) || (oo.getPageNumber() != getPageNumber())) {
            return false; 
        }
        return true;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
