package simpledb.execution;
import java.util.concurrent.*;
import java.util.*;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFileIterator;
import simpledb.storage.StringField;
import simpledb.storage.IntField;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private ConcurrentHashMap<Field, Integer> count_h;
    private int count_val;
    private TupleDesc tupleDesc;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {

        if (what != Op.COUNT) {
            throw new IllegalArgumentException("only support 'count' operator for string");
        }
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;

        this.count_h = new ConcurrentHashMap<Field, Integer>();
        count_val = 0;

        if (gbfield == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggerateValue"});

        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue","aggerateValue"});
        }
    }

    private void mergeTupleByGroup(Tuple tup) {
        Field g_field = (Field) tup.getField(gbfield);

        if (! count_h.containsKey(g_field)) {
            count_h.put(g_field, 1);
        } else {
            int old_count = count_h.get(g_field);
            count_h.put(g_field, old_count+1);
        }
    }

    private void mergeTupleNoGroup(Tuple tup) {
        count_val += 1;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
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
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
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
                    IntField f = new IntField(count_val);
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
                    enu = count_h.keys();
                }

                public boolean hasNext() throws DbException, TransactionAbortedException {
                    return enu.hasMoreElements();
                }

                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    Field c_key = (Field) enu.nextElement();
                    
                    int count_val = count_h.get(c_key);
                    Tuple tp = new Tuple(tupleDesc);


                    IntField f = new IntField(count_val);
                    tp.setField(0, c_key);
                    tp.setField(1, f);
                   return tp;


                }

                public void rewind() throws DbException, TransactionAbortedException {
                    enu = count_h.keys();
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
