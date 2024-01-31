package simpledb.execution;

import java.util.concurrent.*;
import java.util.*;
import java.lang.Math;
import java.lang.reflect.Field;

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
    private ConcurrentHashMap<IntField, Integer> value_h;
    private ConcurrentHashMap<IntField, Integer> count_h;
    private int sum_val;
    private int count_val;
    private TupleDesc tupleDesc;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.value_h = new ConcurrentHashMap<IntField, Integer>();
        this.count_h = new ConcurrentHashMap<IntField, Integer>();
        this.sum_val = 0;
        this.count_val = 0;

        if (gbfield == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggerateValue"});

        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue","aggerateValue"});
        }

    }

    private void mergeTupleByGroup(Tuple tup) {
        IntField g_field = (IntField) tup.getField(gbfield);
        IntField a_field = (IntField) tup.getField(afield);
        int a_val = a_field.getValue();

        if (! value_h.containsKey(g_field)) {
            if (what == Op.COUNT) {
                value_h.put(g_field, 0);
            } else {
                value_h.put(g_field, a_val);
            }
            count_h.put(g_field, 1);

        } else {

            int old_val = value_h.get(g_field);
            int old_count = count_h.get(g_field);
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
            value_h.put(g_field, n_val);
            count_h.put(g_field, old_count+1);
        }
    }

    private void mergeTupleNoGroup(Tuple tup) {
        IntField a_field = (IntField) tup.getField(afield);
        int a_val = a_field.getValue();

        if (count_val == 0) {
            count_val += 1;
            sum_val += a_val;
        } else {
            switch (what) {
                case MIN:
                    sum_val = Math.min(sum_val, a_val);
                    break;
                case MAX:
                    sum_val = Math.max(sum_val, a_val);
                    break;
                case SUM:
                case AVG:
                case SUM_COUNT:
                case SC_AVG:
                    sum_val = sum_val + a_val;
                    break;
            }
            count_val += 1;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            mergeTupleNoGroup(tup);
        } else {
            mergeTupleByGroup(tup);
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
        OpIterator it = null;

        if (gbfield == NO_GROUPING) {
            it = new OpIterator() {
                private boolean used = false;
                public void open() throws DbException, TransactionAbortedException {
                   return;
                }
                public boolean hasNext() throws DbException, TransactionAbortedException {
                    return used == false;
                }

                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    used = true;

                    Tuple tp = new Tuple(tupleDesc);

                    int agg_val = 0;
                    switch (what) {
                        case MIN:
                        case MAX:
                        case SUM:
                            agg_val = sum_val;
                            break;
                        case AVG:
                        case SC_AVG:
                            agg_val = sum_val / count_val;
                            break;
                        case SUM_COUNT:
                            agg_val = count_val;
                            break;
                    }

                    IntField f = new IntField(agg_val);
                    tp.setField(0, f);
                   return tp;
                }

                public void rewind() throws DbException, TransactionAbortedException {
                    used = false;
                }

                public TupleDesc getTupleDesc() {
                    return tupleDesc;
                }

                public void close() {
                    used = false;
                    return;
                }
            };

        } else {
            it = new OpIterator() {
                Enumeration enu;

                public void open() throws DbException, TransactionAbortedException {
                    enu = value_h.keys();
                }

                public boolean hasNext() throws DbException, TransactionAbortedException {
                    return enu.hasMoreElements();
                }

                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    IntField c_key = (IntField) enu.nextElement();
                    
                    int sum_val = value_h.get(c_key);
                    int count_val = count_h.get(c_key);
                    Tuple tp = new Tuple(tupleDesc);

                    int agg_val = 0;
                    switch (what) {
                        case MIN:
                        case MAX:
                        case SUM:
                            agg_val = sum_val;
                            break;
                        case AVG:
                        case SC_AVG:
                            agg_val = sum_val / count_val;
                            break;
                        case SUM_COUNT:
                            agg_val = count_val;
                            break;
                    }

                    IntField f = new IntField(agg_val);
                    tp.setField(0, c_key);
                    tp.setField(1, f);
                   return tp;


                }

                public void rewind() throws DbException, TransactionAbortedException {
                    enu = value_h.keys();
                }

                public TupleDesc getTupleDesc() {
                    return tupleDesc;
                }

                public void close() {
                    return;
                }
            };
        }

        return it;
    }
}
