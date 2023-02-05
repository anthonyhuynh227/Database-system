package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import simpledb.TupleDesc.TDItem;


/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
	private OpIterator child;
	private TupleDesc td;
	private int deleteCount;
	private boolean read;


    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
        this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
        this.deleteCount = 0;
        this.read = false;

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
    	try {
        	while(this.child.hasNext()) {
        		deleteCount++;
        		Database.getBufferPool().deleteTuple(t, child.next());
        	}
    		super.open();
    	}catch(IOException e) {
    		e.printStackTrace();
    	}

    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        read = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(read) {
    		return null;
    	}
    	IntField tupleNum = new IntField(this.deleteCount);
    	Type[] type = {tupleNum.getType()};
    	String[] field = {"Deleted Tuple Count"};
    	TupleDesc desc = new TupleDesc(type, field);
    	Tuple result = new Tuple(desc);
    	result.setField(0, tupleNum);
    	
    	read = true;
    	return result;

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        assert children.length == 1;
        this.child = children[0];
    }

}
