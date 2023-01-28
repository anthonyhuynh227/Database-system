package simpledb;
import java.util.ArrayList;
import java.util.HashMap;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private String groupField;
    private String field;
    private HashMap<Field, Integer> counts;
    private HashMap<Field, Integer> groups;
    private boolean group;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	this.groupField = "";
    	this.field = "";
    	this.counts = new HashMap<>();
    	this.groups = new HashMap<>();
    	if(gbfield == Aggregator.NO_GROUPING) {
    		this.group = false;
    	} else {
    		this.group = true;
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = null;
    	int val = 0;
    	int count = 0;
    	int aggregateVal = 0;
    	String fieldName = tup.getTupleDesc().getFieldName(afield);
    	if(group) {
    		key = tup.getField(gbfield);
    		fieldName = tup.getTupleDesc().getFieldName(gbfield);
    	} else {
    		key = new IntField(Aggregator.NO_GROUPING);
    	}
    	
    	val = ((IntField)tup.getField(afield)).getValue();
    	if(counts.containsKey(key)) {
    		count = counts.get(key);
    	}
    	if(groups.containsKey(key)) {
    		aggregateVal = groups.get(key);
    	} else {
    		if(what == Op.MAX) {
    			counts.put(key, 0);
    			groups.put(key, Integer.MIN_VALUE);
    		} else if (what == Op.MIN) {
    			counts.put(key, 0);
    			groups.put(key, Integer.MAX_VALUE);
    		} else if (what == Op.SUM || what == Op.COUNT || what == Op.AVG) {
    			counts.put(key, 0);
    			groups.put(key, 0);
    		}
    	}
    	aggregateVal = groups.get(key);
    	count = counts.get(key);
    	if (what == Op.MAX) {
    		if(aggregateVal < val) {
    			aggregateVal = val;
    			groups.put(key, val);
    		}
    	} else if (what == Op.MIN) {
    		if(aggregateVal > val) {
    			aggregateVal = val;
    			groups.put(key, val);
    		}
    	} else if (what == Op.SUM) {
    		aggregateVal += val;
    		groups.put(key, aggregateVal);
    	} else if (what == Op.AVG) {
    		aggregateVal += val;
    		groups.put(key, aggregateVal);
    		count++;
    		counts.put(key,count);
    	} else if (what == Op.COUNT){
    		aggregateVal++;
    		groups.put(key, aggregateVal);
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
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc tupleDesc;
        Type[] typeAr;
    	String[] stringAr;

    	if (!group) {
    		typeAr = new Type[1];
    		stringAr = new String[1];
    		typeAr[0] = Type.INT_TYPE;
    		stringAr[0] = field;
    	} else {
    		typeAr = new Type[2];
    		stringAr = new String[2];
    		typeAr[0] = gbfieldtype;
    		typeAr[1] = Type.INT_TYPE;
    		stringAr[0] = groupField;
    		stringAr[1] = field;
    	}
    	tupleDesc = new TupleDesc(typeAr, stringAr);
        if (!group) {
    		for (Field key : groups.keySet()){
    			int value = groups.get(key);
    			if (what == Op.AVG) {
    				value = value/counts.get(key);
    			}
    			Tuple tuple = new Tuple(tupleDesc);
    			tuple.setField(0, new IntField(value));
    			tuples.add(tuple);
    		}
        } else {
        	for (Field key: groups.keySet()) {
        		int value = groups.get(key);
        		if (what == Op.AVG) {
        			value = value / counts.get(key);
        		}
        		Tuple t = new Tuple(tupleDesc);
        		t.setField(0, key);
        		t.setField(1, new IntField(value));
        		tuples.add(t);
        	}
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
