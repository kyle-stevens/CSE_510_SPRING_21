package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;

public class HashJoins extends Iterator{
	
	Scan scan;
	private final String oBucket_prefix = "O_TMP_";
	private final String iBucket_prefix = "I_TMP_";
	private int hash1=4;
	Heapfile outer;
	Heapfile inner;
	
	AttrType[] _outer_in;
	short[] _outer_str_lens;
	
	AttrType[] _inner_in;
	short[] _inner_str_lens;
	
	AttrType[] keypair_in;
	short[] keypair_str_lens;
	
	int outer_join_field;
	int inner_join_field;
	
	int joinOperation;
	
	int n_out_fields;
	FldSpec[] proj_list;
	CondExpr outFilter[];
	CondExpr rightFilter[];
	
	FldSpec[] outer_proj_list;
	
	public HashJoins( AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     n_pages,        
			   String outerRelation,          
			   String innerRelation,      
			   CondExpr outFilter[],      
			   CondExpr rightFilter[],    
			   FldSpec   proj_list[],
			   int        n_out_flds
			   ) throws Exception {
		
		outer = new Heapfile(outerRelation);
		inner = new Heapfile(innerRelation);
		_outer_in = in1.clone();
		_inner_in = in2.clone();
		_outer_str_lens = t1_str_sizes.clone();
		_inner_str_lens = t2_str_sizes.clone();
		
		this.proj_list = proj_list;
		n_out_fields = n_out_flds;

		this.outFilter = outFilter;
		this.rightFilter = rightFilter;
		
		outer_join_field=outFilter[0].operand1.symbol.offset;
		inner_join_field=outFilter[0].operand2.symbol.offset;
		joinOperation = outFilter[0].op.attrOperator;
		
		
		
		Tuple t = new Tuple();
		t.setHdr((short)_outer_in.length, _outer_in, _outer_str_lens);
		
		
		int tuples_in_page = (GlobalConst.MAX_SPACE-HFPage.DPFIXED)/(t.size()+HFPage.SIZE_OF_SLOT);
		hash1 = outer.getRecCnt()/tuples_in_page+1;
		
		t.setHdr((short)_inner_in.length, _inner_in, _inner_str_lens);
		tuples_in_page = (GlobalConst.MAX_SPACE-HFPage.DPFIXED)/(t.size()+HFPage.SIZE_OF_SLOT);
		hash1 = Math.max(hash1,inner.getRecCnt()/tuples_in_page+1);
		
		outer_proj_list = new FldSpec[_outer_in.length];
		for(int i=0;i<_outer_in.length;i++) {
			outer_proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
		}
		
		hashPartition(true);
		for(int hs:list) {
			Heapfile hj = new Heapfile(getHashBucketName(true, hs));
			Scan scan = new Scan(hj);
			Tuple k = null;
			System.out.println("===="+getHashBucketName(true, hs)+"====");
			while((k=scan.getNext(new RID()))!=null) {
				k.setHdr((short)_outer_in.length, _outer_in, _outer_str_lens);
				k.print(_outer_in);
			}
			scan.closescan();
		}
		list = new HashSet<>();
		hashPartition(false);
		for(int hs:list) {
			Heapfile hj = new Heapfile(getHashBucketName(false, hs));
			Scan scan = new Scan(hj);
			Tuple k = null;
			System.out.println("===="+getHashBucketName(false, hs)+"====");
			while((k=scan.getNext(new RID()))!=null) {
				k.setHdr((short)_inner_in.length, _inner_in, _inner_str_lens);
				k.print(_inner_in);
			}
			scan.closescan();
		}
		scan = new Scan(outer);
	}
	
	Iterator joinScan = null;
	
	Set<Integer> list = new HashSet<>();
	private void hashPartition(boolean outer) throws Exception{
		if(outer)
			scan = new Scan(this.outer);
		else scan = new Scan(inner);
		Tuple t = new Tuple();
		while((t=scan.getNext(new RID()))!=null) {
			if(outer)t.setHdr((short)_outer_in.length, _outer_in, _outer_str_lens);
			else t.setHdr((short)_inner_in.length, _inner_in, _inner_str_lens);
			int hash = outer?calculateHashValueForTuple(t, outer_join_field, _outer_in):calculateHashValueForTuple(t, inner_join_field, _inner_in);
			new Heapfile(getHashBucketName(outer, hash)).insertRecord(t.getTupleByteArray());
			list.add(hash);
		}
		scan.closescan();
	}
	
	private String getHashBucketName(boolean outer, int hash) {
		return outer?oBucket_prefix+hash:iBucket_prefix+hash;
	}

	Heapfile currfile_outer;
	Heapfile currfile_inner;
	
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		if(joinScan!=null) {
			Tuple t = joinScan.get_next();
			if(t!=null) {
				return t;
			}
			joinScan.close();
		}
		if(currfile_inner!=null) {
			currfile_inner.deleteFile();
		}
		if(currfile_outer!=null) {
			currfile_outer.deleteFile();
		}
		Tuple t = null;
		while((t = scan.getNext(new RID()))!=null) {
			t.setHdr((short)_outer_in.length, _outer_in, _outer_str_lens);
			int hash = calculateHashValueForTuple(t, outer_join_field, _outer_in);
			currfile_outer = new Heapfile(getHashBucketName(true, hash));
			currfile_inner = new Heapfile(getHashBucketName(false, hash));
			
			joinScan = new NestedLoopsJoins(_outer_in, _outer_in.length, _outer_str_lens, _inner_in, _inner_in.length, _inner_str_lens, 5, 
					new FileScan(getHashBucketName(true, hash),_outer_in,_outer_str_lens,(short)_outer_in.length,_outer_in.length,outer_proj_list,null), 
					getHashBucketName(false, hash), outFilter, rightFilter, proj_list, n_out_fields);
			return get_next();
		}
		scan.closescan();
		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		try {
		if (currfile_inner != null) {
			currfile_inner.deleteFile();
		}
		if (currfile_outer != null) {
			currfile_outer.deleteFile();
		}
		}catch(Exception e) {
			System.out.println("Error deleting heapfiles");
		}
	
	}
	
	private int calculateHashValueForTuple(Tuple t,int joinField, AttrType[] _in) throws Exception{
		int hashValue = -1;
		switch(_in[joinField-1].attrType) {
			case AttrType.attrInteger:
				hashValue = calculateHash(t.getIntFld(joinField));
				break;
			case AttrType.attrString:
				hashValue = calculateHash(t.getStrFld(joinField));
				break;
			case AttrType.attrReal:
				hashValue = calculateHash(t.getFloFld(joinField));
				break;
			default:
				break;
		}
		return hashValue;
	}
	
	private int calculateHash(String data) {
		int hash = 7;
		for (int i = 0; i < data.length(); i++) {
		    hash = hash*11 + data.charAt(i);
		}
		return hash%hash1;
	}
	private int calculateHash(float data) {
		return ((int)data*100)%hash1;
	}
	
	private int calculateHash(int data) {
		return data%hash1;
	}

}
