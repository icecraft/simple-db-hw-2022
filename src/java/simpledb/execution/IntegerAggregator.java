package simpledb.execution;

import java.util.concurrent.*;
import java.util.*;
import java.lang.Math;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFileIterator;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Integer, Integer> value_h;
    private ConcurrentHashMap<Integer, Integer> count_h;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here; MAY DONE
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.value_h = new ConcurrentHashMap<Integer,  Integer>();   
        this.count_h = new ConcurrentHashMap<Integer,  Integer>();   
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfieldtype == null) {
            return;
        } 

        IntField g_field = (IntField) tup.getField(gbfield);
        IntField a_field = (IntField) tup.getField(afield);
        int g_val = g_field.getValue();
        int a_val = a_field.getValue();

        if (! value_h.containsKey(g_val)) {
            if (what == Op.COUNT) {
                value_h.put(g_val, 0);
            } else {
                value_h.put(g_val, a_val);
            }
            count_h.put(g_val, 1);

        } else {

            int old_val = value_h.get(g_val);
            int old_count = count_h.get(g_val);
            int n_val = 0;

            switch (what) {
                case MIN:
                    n_val = Math.min(old_val, a_val);
                    break;
                case MAX:
                    n_val = Math.max(old_val, a_val);
                    break;
                case SUM:
                case AVG:
                case SUM_COUNT:
                case SC_AVG:
                    n_val = old_val + a_val;
                    break;
                default:
                    break;
            }
            value_h.put(g_val, n_val);
            count_h.put(g_val, old_count+1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        throw new
        UnsupportedOperationException("please implement me for lab2");
    }

    private static final class IntegerAggregatorIterator implements OpIterator {
        private ConcurrentHashMap<Integer, Integer> value_h;
        private ConcurrentHashMap<Integer, Integer> count_h;
        private Op what;
        private Enumeration enu;

        public IntegerAggregatorIterator( ConcurrentHashMap<Integer, Integer>value_h,  ConcurrentHashMap<Integer, Integer> count_h, Op what ) {
            this.value_h = value_h;
            this.count_h = count_h;
            this.what = what;
        }
    
        public void open() {
            enu = value_h.keys();
        }


        public boolean hasNext() throws DbException, TransactionAbortedException {
            return enu.hasMoreElements();
        }


        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return null;
        }

        public void rewind() throws DbException, TransactionAbortedException {
            enu = value_h.keys();
        }


        public TupleDesc getTupleDesc() {
            // TODO:
            return null;
        }


        public void close() {
            return;
        }
    }

}
