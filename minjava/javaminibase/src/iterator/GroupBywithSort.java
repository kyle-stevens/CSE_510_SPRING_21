package iterator;

import java.io.IOException;
import java.util.Arrays;

import global.AggType;
import global.AttrType;
import global.IndexType;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import index.ClusteredBtreeIndexScan;
import index.IndexException;
import index.IndexScan;

public class GroupBywithSort extends Iterator{

	Iterator scan;
	FldSpec[] projection;
	AggType _agg_type;
	int _group_by_attr;
	
	AttrType[] _in1;
	int len_in;
	short[] t1_str_sizes;
	
	int n_pages;
	
	int agg_list[];

	
	public GroupBywithSort (
			AttrType[] in1, int len_in1, short[] t1_str_sizes,
			String relationName,
			int group_by_attr,
			int[] agg_list,
			AggType agg_type,
			int n_pages,
			boolean indexExists,
			String indexFileName) throws Exception{

		_group_by_attr = group_by_attr;
		_agg_type = agg_type;
		projection = new FldSpec[len_in1];
		
		this.agg_list = agg_list;
		this.n_pages = n_pages;
		
		
		this.t1_str_sizes = t1_str_sizes;
		len_in = len_in1;
		_in1 = in1.clone();
		
		for (int i = 0; i < len_in; i++) {
		     projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		if(indexExists) {
			scan = new ClusteredBtreeIndexScan(indexFileName, in1,
		              t1_str_sizes, null, group_by_attr);
		}else{
			Iterator am = new FileScan(relationName,in1,t1_str_sizes,(short)len_in1,len_in1,projection,null);
			int size = 4;
			if(in1[group_by_attr-1].attrType==AttrType.attrString) {
				size = t1_str_sizes[0];
			}
			scan = new Sort(in1, (short)len_in1, t1_str_sizes, am, group_by_attr, new TupleOrder(TupleOrder.Ascending), size , 40);
	}
	}

	@Override
	public Tuple get_next() throws Exception {
		
		// TODO Auto-generated method stub
		switch(_agg_type.agg_type) {
		case AggType.aggAvg:
			return getAvg();
		case AggType.aggMax:
			return getMax();
		case AggType.aggMin:
			return getMin();
		}
		return getSky();
	}
	Tuple curr_tuple = null;
	
	public Tuple getMax() throws Exception{
		float result[] = new float[agg_list.length];
		Arrays.fill(result, Float.MIN_VALUE);
		if(curr_tuple==null) {
			curr_tuple = scan.get_next();
			if(curr_tuple!=null)
				curr_tuple = new Tuple(curr_tuple);
		}
		if (curr_tuple != null) {
			setHdr(curr_tuple);
			updateMaxResult(result, curr_tuple);
			Tuple nextTuple = scan.get_next();
			if(nextTuple!=null) {
				nextTuple = new Tuple(nextTuple);
				setHdr(nextTuple);
			}
			while(nextTuple!=null && (TupleUtils.CompareTupleWithTuple(_in1[_group_by_attr-1], curr_tuple, _group_by_attr, nextTuple, _group_by_attr)==0)) {
				
				updateMaxResult(result, nextTuple);
				nextTuple = scan.get_next();
				if(nextTuple==null)break;
				nextTuple = new Tuple(nextTuple);
				setHdr(nextTuple);
			}
			Tuple agg_rslt = createTupleFromResult(result,curr_tuple);
			curr_tuple = nextTuple;
			return agg_rslt;
		}
		return curr_tuple;
	}
	
	public Tuple getMin() throws Exception{
		float result[] = new float[agg_list.length];
		Arrays.fill(result, Float.MAX_VALUE);
		if(curr_tuple==null) {
			curr_tuple = scan.get_next();
			if(curr_tuple!=null)
				curr_tuple = new Tuple(curr_tuple);
		}
		if (curr_tuple != null) {
			setHdr(curr_tuple);
			updateMinResult(result, curr_tuple);
			Tuple nextTuple = scan.get_next();
			if(nextTuple!=null) {
				nextTuple = new Tuple(nextTuple);
				setHdr(nextTuple);
			}
			while(nextTuple!=null && (TupleUtils.CompareTupleWithTuple(_in1[_group_by_attr-1], curr_tuple, _group_by_attr, nextTuple, _group_by_attr)==0)) {
				
				updateMinResult(result, nextTuple);
				nextTuple = scan.get_next();
				if(nextTuple==null)break;
				nextTuple = new Tuple(nextTuple);
				setHdr(nextTuple);
			}
			Tuple agg_rslt = createTupleFromResult(result,curr_tuple);
			curr_tuple = nextTuple;
			return agg_rslt;
		}
		return curr_tuple;
	}
	
	public Tuple getAvg() throws Exception{
		float result[] = new float[agg_list.length];
		if(curr_tuple==null) {
			curr_tuple = scan.get_next();
			if(curr_tuple!=null)
				curr_tuple = new Tuple(curr_tuple);
		}
		if (curr_tuple != null) {
			int i=1;
			setHdr(curr_tuple);
			updateAvgResult(result, curr_tuple,i++);
			Tuple nextTuple = scan.get_next();
			if(nextTuple!=null) {
				nextTuple = new Tuple(nextTuple);
				setHdr(nextTuple);
			}
			while(nextTuple!=null && (TupleUtils.CompareTupleWithTuple(_in1[_group_by_attr-1], curr_tuple, _group_by_attr, nextTuple, _group_by_attr)==0)) {
				
				updateAvgResult(result, nextTuple,i++);
				nextTuple = scan.get_next();
				if(nextTuple==null)break;
				nextTuple = new Tuple(nextTuple);
				setHdr(nextTuple);
			}
			Tuple agg_rslt = createTupleFromResult(result,curr_tuple);
			curr_tuple = nextTuple;
			return agg_rslt;
		}
		return curr_tuple;
	}
	
	private void setHdr(Tuple t) throws Exception{
		t.setHdr((short)len_in, _in1, t1_str_sizes);
	}
	
	private Tuple createTupleFromResult(float[] result, Tuple from) throws Exception{
		int number_cols = result.length+1;
		AttrType[] in = new AttrType[number_cols];
		in[0] = _in1[_group_by_attr-1];
		for(int i=0;i<result.length;i++) {
			in[i+1] = new AttrType(AttrType.attrReal);
		}
		short[] ssizes = null;
		if(in[0].attrType==AttrType.attrString) {
			ssizes = new short[1];
			ssizes[0] = t1_str_sizes[0];
		}
		Tuple t = new Tuple();
		t.setHdr((short)number_cols, in, ssizes);
		t = new Tuple(t.size());
		t.setHdr((short) number_cols, in, ssizes);
		switch (in[0].attrType) {
		case AttrType.attrInteger:
			t.setIntFld(1, from.getIntFld(_group_by_attr));
			break;
		case AttrType.attrString:
			t.setStrFld(1, from.getStrFld(_group_by_attr));
			break;
		case AttrType.attrReal:
			t.setFloFld(1, from.getFloFld(_group_by_attr));
			break;
		}
		for(int i=0;i<result.length;i++) {
			t.setFloFld(i+2, Math.round(result[i]*10000)/10000.0f);
		}
		return t;
	}
	
	private Tuple createTupleFromTuple(Tuple skyline, Tuple from) throws Exception{
		int number_cols = agg_list.length+1;
		AttrType[] in = new AttrType[number_cols];
		in[0] = _in1[_group_by_attr-1];
		for(int i=0;i<in.length-1;i++) {
			switch(_in1[agg_list[i]-1].attrType) {
			case AttrType.attrInteger:
				in[i+1] = new AttrType(AttrType.attrInteger);
				break;
			case AttrType.attrReal:
				in[i+1] = new AttrType(AttrType.attrReal);
				break;
			}
		}
		short[] ssizes = null;
		if(in[0].attrType==AttrType.attrString) {
			ssizes = new short[1];
			ssizes[0] = t1_str_sizes[0];
		}
		Tuple t = new Tuple();
		t.setHdr((short)number_cols, in, ssizes);
		t = new Tuple(t.size());
		t.setHdr((short) number_cols, in, ssizes);
		switch (in[0].attrType) {
		case AttrType.attrInteger:
			t.setIntFld(1, from.getIntFld(_group_by_attr));
			break;
		case AttrType.attrString:
			t.setStrFld(1, from.getStrFld(_group_by_attr));
			break;
		case AttrType.attrReal:
			t.setFloFld(1, from.getFloFld(_group_by_attr));
			break;
		}
		for(int i=1;i<in.length;i++) {
			switch(in[i].attrType) {
			case AttrType.attrInteger:
				t.setIntFld(i+1,skyline.getIntFld(agg_list[i-1]));
				break;
			case AttrType.attrReal:
				t.setFloFld(i+1,skyline.getFloFld(agg_list[i-1]));
				break;
			}
		}
		return t;
	}
	
	private void updateMaxResult(float[] result, Tuple t) throws Exception{
		for (int i = 0; i < result.length; i++) {
			switch (_in1[agg_list[i] - 1].attrType) {
			case AttrType.attrInteger:
				result[i] = Math.max(result[i],t.getIntFld(agg_list[i]));
				break;
			case AttrType.attrReal:
				result[i] = Math.max(result[i],t.getFloFld(agg_list[i]));
				break;
			}
		}
	}
	
	private void updateMinResult(float[] result, Tuple t) throws Exception{
		for (int i = 0; i < result.length; i++) {
			switch (_in1[agg_list[i] - 1].attrType) {
			case AttrType.attrInteger:
				result[i] = Math.min(result[i],t.getIntFld(agg_list[i]));
				break;
			case AttrType.attrReal:
				result[i] = Math.min(result[i],t.getFloFld(agg_list[i]));
				break;
			}
		}
	}
	
	private void updateAvgResult(float[] result, Tuple t, int count) throws Exception{
		for (int i = 0; i < result.length; i++) {
			float curr = 0;
			switch (_in1[agg_list[i] - 1].attrType) {
			case AttrType.attrInteger:
				curr = t.getIntFld(agg_list[i]);
				break;
			case AttrType.attrReal:
				curr = t.getFloFld(agg_list[i]);
				break;
			}
			result[i] = (result[i]*(count-1)+curr)/count;
		}
	}
	
	private Iterator sky2 = null;
	private Tuple skyNextTuple = null;
	
	public Tuple getSky() throws Exception{
		Tuple t = null;
		if(sky2!=null && (t=sky2.get_next())!=null) {
			t = new Tuple(t);
			setHdr(t);
			return createTupleFromTuple(t,curr_tuple);
			
		}
		if(sky2!=null) {
			sky2.close();
			sky2 = null;
		}
		curr_tuple = skyNextTuple;
		Heapfile hf = new Heapfile(null);
		if(curr_tuple==null) {
			curr_tuple = scan.get_next();
			if(curr_tuple!=null)
				curr_tuple = new Tuple(curr_tuple);
		}
		if (curr_tuple != null) {
			setHdr(curr_tuple);
			hf.insertRecord(curr_tuple.getTupleByteArray());
			Tuple nextTuple = scan.get_next();
			if(nextTuple!=null) {
				nextTuple = new Tuple(nextTuple);
				setHdr(nextTuple);
			}
			while(nextTuple!=null && (TupleUtils.CompareTupleWithTuple(_in1[_group_by_attr-1], curr_tuple, _group_by_attr, nextTuple, _group_by_attr)==0)) {
				hf.insertRecord(nextTuple.getTupleByteArray());
				nextTuple = scan.get_next();
				if(nextTuple==null)break;
				nextTuple = new Tuple(nextTuple);
				setHdr(nextTuple);
			}
			sky2 = new BlockNestedLoopSky(_in1, _in1.length, t1_str_sizes,
		               null, hf.getName(), agg_list, agg_list.length, 5);
			skyNextTuple = nextTuple;
			return getSky();
		}
		return curr_tuple;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		if(scan!=null) {
			scan.close();
		}
	}
	
	
}
