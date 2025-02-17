package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */

    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple t1;

    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // TODO: some code goes here
        this.p = p;
        
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // TODO: some code goes here
        return p;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField1Name() {
        // TODO: some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }
    /**
     * @return the field name of join field2. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField2Name() {
        // TODO: some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *         implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        child1.open();
        child2.open();
        t1 = null;

    }

    public void close() {
        // TODO: some code goes here
        t1 = null;
        child2.close();
        child1.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        child1.rewind();
        child2.rewind();
        t1 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (child1.hasNext() || t1  != null) {
            if (t1 == null && child1.hasNext()) {
                t1 = child1.next();
            }
            while (child2.hasNext()) {
                Tuple t2 = child2.next();
                if (p.filter(t1, t2)) {
                    Tuple res = new Tuple(getTupleDesc());
                    for (int i=0; i < t1.getTupleDesc().numFields(); i++) {
                        res.setField(i, t1.getField(i));
                    }
                    for (int i=0; i < t2.getTupleDesc().numFields(); i++) {
                        res.setField(i + t1.getTupleDesc().numFields(), t2.getField(i));
                    }
                    res.setRecordId(t1.getRecordId());
                    return res;
                }
            }

            t1 = null;
            child2.rewind();
        }

        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

}
