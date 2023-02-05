package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate predicate;
    private OpIterator child1OpIterator;
    private OpIterator child2OpIterator;
    private Tuple Tuple1;
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.predicate = p;
        this.child1OpIterator = child1;
        this.child2OpIterator = child2;
        this.Tuple1 = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.predicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1OpIterator.getTupleDesc().getFieldName(predicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2OpIterator.getTupleDesc().getFieldName(predicate.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1OpIterator.getTupleDesc(), child2OpIterator.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1OpIterator.open();
        child2OpIterator.open();
        super.open();
        Tuple1 = child1OpIterator.next();
    }

    public void close() {
        // some code goes here
        super.close();
        child1OpIterator.close();
        child2OpIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1OpIterator.rewind();
        child2OpIterator.rewind();
        Tuple1 = child1OpIterator.next();
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
        // some code goes here
        while (Tuple1 != null) {
            while (child2OpIterator.hasNext()) {
                Tuple tuple2 = child2OpIterator.next();
                if (this.predicate.filter(Tuple1, tuple2)) {
                    // Fill in the value for the tuple and return
                    Tuple tuple = new Tuple(this.getTupleDesc());
                    for (int i = 0; i < Tuple1.getTupleDesc().numFields(); i++) {
                        tuple.setField(i, Tuple1.getField(i));
                    }
                    for (int i = 0; i < tuple2.getTupleDesc().numFields(); i++) {
                        tuple.setField(i + Tuple1.getTupleDesc().numFields(), tuple2.getField(i));
                    }
                    return tuple;
                }
            }
            if (child1OpIterator.hasNext()) {
                Tuple1 = child1OpIterator.next();
            } else {
                Tuple1 = null;
            }
            child2OpIterator.rewind();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child1OpIterator, this.child2OpIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1OpIterator = children[0];
        this.child2OpIterator = children[1];
    }

}