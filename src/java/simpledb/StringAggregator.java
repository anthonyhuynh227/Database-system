package simpledb;
import java.util.ArrayList;
import java.util.HashMap;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private HashMap<Field, Integer> agg;
    private boolean group;
    private String fieldName;
    private String fieldGroup;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT) {
    		throw new IllegalStateException();
    	}
    	this.gbfield = gbfield;
    	if(this.gbfield == Aggregator.NO_GROUPING) {
    		this.group = false;
    	} else {
    		this.group = true;
    	}
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.agg = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        fieldName = tup.getTupleDesc().getFieldName(afield);
    	Field gb;
    	if(gbfield != NO_GROUPING){
    		gb = tup.getField(gbfield);
    		fieldGroup = tup.getTupleDesc().getFieldName(gbfield);
    	} else {
    		gb = null;
    	}
    	if(agg.get(gb) == null){
    		agg.put(gb, 1);
    	} else {
    		agg.put(gb, agg.get(gb)+1);
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc tupleDesc;
    	Type[] typeAr;
    	String[] stringAr;

    	if (!group) {
    		typeAr = new Type[1];
    		stringAr = new String[1];
    		typeAr[0] = Type.INT_TYPE;
    		stringAr[0] = fieldName;//don't actually need real field name
    	} else {
    		typeAr = new Type[2];
    		stringAr = new String[2];
    		typeAr[0] = gbfieldtype;
    		typeAr[1] = Type.INT_TYPE;
    		stringAr[0] = fieldGroup;
    		stringAr[1] = fieldName;
    	}
    	tupleDesc = new TupleDesc(typeAr, stringAr);
        
        if (!group){
        	for (Field key : agg.keySet()){
        		int value = agg.get(key);
        		Tuple tp = new Tuple(tupleDesc);
        		tp.setField(0, new IntField(value));
        		tuples.add(tp);
        	}
        }else{
        	for (Field key:agg.keySet()){
        		int value = agg.get(key);
        		
        		Tuple e = new Tuple(tupleDesc);
        		e.setField(0, key);
        		e.setField(1, new IntField(value));
        		tuples.add(e);
        	}
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
