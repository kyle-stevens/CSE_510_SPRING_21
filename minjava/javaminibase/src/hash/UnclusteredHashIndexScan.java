package hash;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

public class UnclusteredHashIndexScan extends Iterator {
	
	Heapfile indexFile;
	Heapfile relationFile;
	
	
	AttrType[] _in;
	short[] _sSizes;
	
	AttrType[] _dir_page_attr;
	short[] _dir_sizes;
	
	AttrType[] _keyPageAttr;
	
	private AttrType keyFileAttr[];
    private short[] keyFileStrLens;
	
	int index_attr;
	
	boolean indexOnly = false;
	
	
	public UnclusteredHashIndexScan(String directoryFile, AttrType[] in, short[] sSizes, int attr_no, String relationName, boolean indexOnly) throws Exception{
		indexFile = new Heapfile(directoryFile);
		relationFile = new Heapfile(relationName);

		_in = in.clone();
		_sSizes = sSizes.clone();
		
		this.indexOnly = indexOnly;
		this.index_attr = attr_no;
		
		_dir_page_attr = new AttrType[1];
		_dir_page_attr[0] = new AttrType(AttrType.attrString);
		_dir_sizes = new short[1];
		_dir_sizes[0] = GlobalConst.MAX_NAME + 2;
		
		keyFileAttr = new AttrType[2];
        keyFileAttr[0] = _in[attr_no-1];
        keyFileAttr[1] = new AttrType(AttrType.attrString);
        
        if(_in[attr_no-1].attrType==AttrType.attrString) {
        	keyFileStrLens = new short[2];
        	keyFileStrLens[0] = sSizes[0];
        	keyFileStrLens[1] = GlobalConst.MAX_NAME+2;
        }else {
        	keyFileStrLens = new short[1];
        	keyFileStrLens[0] = GlobalConst.MAX_NAME+2;
        	
        }
		
		_keyPageAttr = new AttrType[2];
		_keyPageAttr[0] = new AttrType(AttrType.attrInteger);
		_keyPageAttr[1] = new AttrType(AttrType.attrInteger);
		
			
	}
	
	String curr_page_file = null;
	String curr_key_page_file = null;
	RID curr_key_page_RID = null;
	RID curr_hash_bucket_RID = null;
	Scan scan;
	
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		Tuple t = null;
		
		if(curr_page_file!=null){
			if(scan==null) {
				Heapfile hf = new Heapfile(curr_page_file);
				scan = new Scan(hf);
			}			
			t = scan.getNext(new RID());
			if(t==null) {
				curr_page_file = null;
				scan.closescan();
				scan = null;
				return get_next();
			}
			t.setHdr((short)_keyPageAttr.length, _keyPageAttr, null);
			return relationFile.getRecord(ridFromTuple(t));
		}else if(curr_key_page_file!=null) {
			Scan scan = new Scan(new Heapfile(curr_key_page_file));
			if(curr_key_page_RID!=null){
				scan.position(curr_key_page_RID);
				scan.getNext(curr_key_page_RID);
			}else {
				curr_key_page_RID = new RID();
			}
			t = scan.getNext(curr_key_page_RID);
			if(t==null) {
				curr_key_page_RID = null;
				curr_key_page_file = null;
				scan.closescan();
				return get_next();
				
				//this hashbucket is done, fetch next one
				//call getNext()
			}
			t.setHdr((short)keyFileAttr.length, keyFileAttr, keyFileStrLens);
			t = new Tuple(t);
			t.setHdr((short)keyFileAttr.length, keyFileAttr, keyFileStrLens);
			scan.closescan();
			curr_page_file = t.getStrFld(2);
			return get_next();
		}else {
			Scan scan = new Scan(indexFile);
			if(curr_hash_bucket_RID!=null) {
				scan.position(curr_hash_bucket_RID);
				scan.getNext(curr_hash_bucket_RID);
			}else {
				curr_hash_bucket_RID = new RID();
			}
			t = scan.getNext(curr_hash_bucket_RID);
			if(t==null) {
				scan.closescan();
				return null;
			}
			t.setHdr((short)_dir_page_attr.length, _dir_page_attr, _dir_sizes);
			t = new Tuple(t);
			t.setHdr((short)_dir_page_attr.length, _dir_page_attr, _dir_sizes);
			scan.closescan();
			curr_key_page_file = t.getStrFld(1);
			return get_next();
		}
	}
	private RID ridFromTuple(Tuple t1) throws Exception {
	    try {
	      t1.setHdr((short) 2, _keyPageAttr, null);
	    } catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      e.printStackTrace();
	    }
	    RID rid = new RID();
	    rid.slotNo = t1.getIntFld(2);
		rid.pageNo = new PageId(t1.getIntFld(1));
		return rid;
	}
	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		if(scan!=null) {
			scan.closescan();
		}
	}

}
