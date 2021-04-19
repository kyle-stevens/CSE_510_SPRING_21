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

public class HashJoins extends Iterator {

	Scan oScan;
	Scan iScan;
	private final String iBucket_prefix = "I_TMP_";
	private int hash1 = 4;
	Heapfile outer;
	Heapfile inner;
	Heapfile innerHash = new Heapfile("tmp_inner_hash");

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
	Tuple Jtuple;

	FldSpec[] outer_proj_list;

	public HashJoins(AttrType in1[], int len_in1, short t1_str_sizes[], AttrType in2[], int len_in2,
			short t2_str_sizes[], int n_pages, String outerRelation, String innerRelation, CondExpr outFilter[],
			CondExpr rightFilter[], FldSpec proj_list[], int n_out_flds) throws Exception {

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

	      done  = false;
	      get_from_outer = true;
	      Jtuple = new Tuple();
		outer_join_field = outFilter[0].operand1.symbol.offset;
		inner_join_field = outFilter[0].operand2.symbol.offset;
		joinOperation = outFilter[0].op.attrOperator;

		Tuple t = new Tuple();
		t.setHdr((short) _outer_in.length, _outer_in, _outer_str_lens);

		int tuples_in_page = (GlobalConst.MAX_SPACE - HFPage.DPFIXED) / (t.size() + HFPage.SIZE_OF_SLOT);
		hash1 = outer.getRecCnt() / tuples_in_page + 1;

		t.setHdr((short) _inner_in.length, _inner_in, _inner_str_lens);
		tuples_in_page = (GlobalConst.MAX_SPACE - HFPage.DPFIXED) / (t.size() + HFPage.SIZE_OF_SLOT);
		hash1 = Math.max(hash1, inner.getRecCnt() / tuples_in_page + 1);

		outer_proj_list = new FldSpec[_outer_in.length];
		for (int i = 0; i < _outer_in.length; i++) {
			outer_proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}

		hashPartition();
		AttrType[] Jtypes = new AttrType[n_out_flds];
		TupleUtils.setup_op_tuple(Jtuple, Jtypes,
				   in1, len_in1, in2, len_in2,
				   t1_str_sizes, t2_str_sizes,
				   proj_list, n_out_flds);
		oScan = new Scan(outer);
	}

	Iterator joinScan = null;

	Set<Integer> list = new HashSet<>();

	private void hashPartition() throws Exception {
		oScan = new Scan(inner);
		Tuple t = new Tuple();
		while ((t = oScan.getNext(new RID())) != null) {
			t.setHdr((short) _inner_in.length, _inner_in, _inner_str_lens);
			int hash = calculateHashValueForTuple(t, inner_join_field, _inner_in);
			new Heapfile(getHashBucketName(hash)).insertRecord(t.getTupleByteArray());
			Tuple tmp = new Tuple();
			AttrType[] types = { new AttrType(AttrType.attrInteger) };
			tmp.setHdr((short) 1, types, null);
			tmp = new Tuple(tmp.size());
			tmp.setHdr((short) 1, types, null);
			tmp.setIntFld(1, hash);
			Scan tmp_scan = new Scan(innerHash);
			Tuple tt = null;
			boolean flag = false;
			while ((tt = tmp_scan.getNext(new RID())) != null) {
				tt.setHdr((short) 1, types, null);
				if (tt.getIntFld(1) == hash) {
					tmp_scan.closescan();
					flag = true;
					break;
				}
			}
			tmp_scan.closescan();
			if (flag)
				continue;
			innerHash.insertRecord(tmp.getTupleByteArray());
//			list.add(hash);
		}
		oScan.closescan();
	}

	private String getHashBucketName(int hash) {
		return iBucket_prefix + hash;
	}
	 private   boolean        done,         // Is the join complete
	    get_from_outer;  

     Tuple t = null;
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		 if (done)
				return null;
			      
			      do
				{
				  // If get_from_outer is true, Get a tuple from the outer, delete
				  // an existing scan on the file, and reopen a new scan on the file.
				  // If a get_next on the outer returns DONE?, then the nested loops
				  //join is done too.
				  
				  if (get_from_outer == true)
				    {
				      get_from_outer = false;
				      if (iScan != null)     // If this not the first time,
					{
					  // close scan
				    	  iScan.closescan();
					}
				    
				     
				      if ((t=oScan.getNext(new RID())) == null)
					{
					  done = true;
					  if (iScan != null) 
					    {
						  iScan.closescan();
					      inner = null;
					    }
					  
					  return null;
					}
				      t.setHdr((short)_outer_in.length, _outer_in, _outer_str_lens);
				      iScan = new Scan(new Heapfile(getHashBucketName(calculateHashValueForTuple(t, outer_join_field, _outer_in))));
				    }  // ENDS: if (get_from_outer == TRUE)
				 
				  
				  // The next step is to get a tuple from the inner,
				  // while the inner is not completely scanned && there
				  // is no match (with pred),get a tuple from the inner.
				  
				 
				      RID rid = new RID();
				      Tuple inner_tuple = null;
				      while ((inner_tuple = iScan.getNext(rid)) != null)
					{
					  inner_tuple.setHdr((short)_inner_in.length, _inner_in,_inner_str_lens);
					  
					      if (PredEval.Eval(outFilter, t, inner_tuple, _outer_in, _inner_in) == true)
						{
						  // Apply a projection on the outer and inner tuples.
						  Projection.Join(t, _outer_in, 
								  inner_tuple, _inner_in, 
								  Jtuple, proj_list, n_out_fields);
						  return Jtuple;
						}
					}
				      
				      // There has been no match. (otherwise, we would have 
				      //returned from t//he while loop. Hence, inner is 
				      //exhausted, => set get_from_outer = TRUE, go to top of loop
				      
				      get_from_outer = true; // Loop back to top and get next outer tuple.	      
				} while (true);
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		try {
			if(oScan!=null)
				oScan.closescan();
			if(iScan!=null)
				iScan.closescan();
			oScan = new Scan(innerHash);
			Tuple t = null;
			AttrType[] types = { new AttrType(AttrType.attrInteger) };

			while ((t = oScan.getNext(new RID())) != null) {
				t.setHdr((short) 1, types, null);
				new Heapfile(getHashBucketName(t.getIntFld(1))).deleteFile();
			}
			oScan.closescan();
			innerHash.deleteFile();

		} catch (Exception e) {
			System.out.println("Error deleting heapfiles");
		}

	}

	private int calculateHashValueForTuple(Tuple t, int joinField, AttrType[] _in) throws Exception {
		int hashValue = -1;
		switch (_in[joinField - 1].attrType) {
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
		long hash = 7;
		for (int i = 0; i < data.length(); i++) {
			hash = hash * 11 + data.charAt(i);
		}
		return (int)(hash % hash1);
	}

	private int calculateHash(float data) {
		return ((int)(data * 100) )% hash1;
	}

	private int calculateHash(int data) {
		return data % hash1;
	}

}
