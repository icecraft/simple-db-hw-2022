package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tdesc;
    private RecordId rId;
    private Field[] _fields;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // TODO: some code goes here
        this.tdesc = td;
        this._fields = new Field[td.getSize()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here; MAY DONE
        return this.tdesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // TODO: some code goes here; MAY DONE
        return this.rId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // TODO: some code goes here; MAY DONE 
        this.rId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // TODO: some code goes here; MAY DONE
        if (i >= 0 && this._fields.length > i) {
            this._fields[i] = f;
        }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // TODO: some code goes here; MAY DONE
        if (i >=0 && this._fields.length > i) {
            return this._fields[i];
        }
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // TODO: some code goes here; MAY DONE
        String[] arr = new String[this._fields.length];
        for (int i = 0; i < this._fields.length; i++) {
            arr[i] = String.format("%s", this._fields[i]);
        }
        return String.format("<p>\n%s\n</p>", String.join("\t", arr));
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // TODO: some code goes here; MAY DONE
        int maxSize = this._fields.length;
        Field[] items = this._fields;
        Iterator<Field> it = new Iterator<Field>() {
            private int currentIndex = 0;
 
            public boolean hasNext() {
                return currentIndex < maxSize;
            }
        
            public Field next() {
                return items[currentIndex++];
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return it;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // TODO: some code goes here; MAY DONE
        this.tdesc = td;
    }
}

