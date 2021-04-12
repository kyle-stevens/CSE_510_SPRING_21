package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class TopK_HashJoin  extends Iterator{
	
	AttrType[] outer_in;
	short[] outer_str_lens;
	
	AttrType[] inner_in;
	short[] inner_str_lens;
	
	int outer_join_attr;
	int inner_join_attr;
	
	int outer_merge_attr;
	int inner_merge_attr;
	
	int n_pages;
	int k;
	
	Iterator am1;
	
	public TopK_HashJoin(
			AttrType[] in1, int len_in1, short[] t1_str_sizes,
			int joinAttr1,
			int mergeAttr1,
			AttrType[] in2, int len_in2, short[] t2_str_sizes,
			int joinAttr2,
			int mergeAttr2,
			String relationName1,
			String relationName2,
			int n_out_fields,
			int pref_list_length,
			int[] pref_list,
			FldSpec[] output_proj_list,
			AttrType[] output_in,
			short[] output_str_lens,
			int k,
			int n_pages
			) throws Exception
	{
		this.outer_in = in1.clone();
		this.inner_in = in2.clone();
		this.outer_str_lens = t1_str_sizes.clone();
		this.inner_str_lens = t2_str_sizes.clone();
		this.outer_join_attr = joinAttr1;
		this.inner_join_attr = joinAttr2;
		this.outer_merge_attr = mergeAttr1;
		this.inner_merge_attr = mergeAttr2;
		this.n_pages = n_pages;
		this.k = k;
		
		CondExpr[] outFilter = new CondExpr[2];

		outFilter[0] = new CondExpr();
		outFilter[0].next  = null;
		outFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);	
		
		outFilter[1] = null;
		
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),outer_join_attr);
		
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),inner_join_attr);
		
		
		HashJoins hj = new HashJoins(outer_in, len_in1, outer_str_lens, inner_in, len_in2, t2_str_sizes, n_pages, relationName1, relationName2, outFilter, null, output_proj_list, n_out_fields);
		am1 = new SortPref(output_in,(short)n_out_fields,output_str_lens,hj,new TupleOrder(TupleOrder.Descending),pref_list,pref_list_length,n_pages);
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		if(k--==0)return null;
		return am1.get_next();
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		am1.close();
	}
}
