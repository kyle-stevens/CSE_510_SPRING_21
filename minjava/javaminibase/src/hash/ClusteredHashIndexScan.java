package hash;

import java.io.IOException;

import bufmgr.PageNotReadException;
import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFPage;
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

public class ClusteredHashIndexScan extends Iterator{

	Heapfile indexFile;
	
	
	AttrType[] _in;
	short[] _sSizes;
	
	AttrType[] _dir_page_attr;
	short[] _dir_sizes;
	
	AttrType[] _keyPageAttr;
	short[] _key_sizes;
	
	
	int index_field;
	
	public ClusteredHashIndexScan(String directoryFile, AttrType[] in, short[] sSizes, int attr_no) throws Exception{
		
		index_field = attr_no;
		
		indexFile = new Heapfile(directoryFile);
		_in = in.clone();
		_sSizes = sSizes.clone();
		
		_dir_page_attr = new AttrType[1];
		_dir_page_attr[0] = new AttrType(AttrType.attrString);
		_dir_sizes = new short[1];
		_dir_sizes[0] = GlobalConst.MAX_NAME + 2;
		
		
		_keyPageAttr = new AttrType[2];
		_keyPageAttr[0] = new AttrType(in[attr_no-1].attrType);
		_keyPageAttr[1] = new AttrType(AttrType.attrInteger);
		
		if(in[attr_no-1].attrType == AttrType.attrString) {
			_key_sizes = new short[1];
			_key_sizes[0] = sSizes[0];
		}
		
	}
	
	HFPage curr_page = null;
	RID curr_page_RID = null;
	
	Scan currBucketScan = null;
	
	RID curr_hash_bucket_RID = null;
	
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		Tuple t = null;
		
		if(curr_page!=null) {
			if(curr_page_RID!=null) {
				curr_page_RID = curr_page.nextRecord(curr_page_RID);
			}else {
				curr_page_RID = curr_page.firstRecord();
			}
			if(curr_page_RID==null) {
				unpinPage(curr_page.getCurPage(), false);
				PageId next_page = curr_page.getNextPage();
				if(next_page.pid!=GlobalConst.INVALID_PAGE) {
					Page page = new Page();
					pinPage(next_page, page, false);
					curr_page = new HFPage(page);
				}else {
					curr_page = null;
				}
				return get_next();
			}
			t = curr_page.getRecord(curr_page_RID);
			t.setHdr((short)_in.length, _in, _sSizes);
			t = new Tuple(t);
			t.setHdr((short)_in.length, _in, _sSizes);
			System.out.println(curr_page_RID.pageNo.pid);
			return t;
		}else if(currBucketScan!=null){
			t = currBucketScan.getNext(new RID());
			if(t==null) {
				currBucketScan.closescan();
				currBucketScan = null;
				return get_next();				
			}
			t.setHdr((short)_keyPageAttr.length, _keyPageAttr, _key_sizes);
			t = new Tuple(t);
			t.setHdr((short)_keyPageAttr.length, _keyPageAttr, _key_sizes);
			PageId pid = new PageId(t.getIntFld(2));
			Page page = new Page();
			pinPage(pid,page,false);
			curr_page = new HFPage(page);
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
			currBucketScan = new Scan(new Heapfile(t.getStrFld(1)));
			return get_next();
		}
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		
	}
	
	/**
	   * short cut to access the pinPage function in bufmgr package.
	   * @see bufmgr.pinPage
	   */
	  private void pinPage(PageId pageno, Page page, boolean emptyPage)
	    throws HFBufMgrException {
	    
	    try {
	      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
	    }
	    catch (Exception e) {
	      throw new HFBufMgrException(e,"Heapfile.java: pinPage() failed");
	    }
	    
	  } // end of pinPage

	  /**
	   * short cut to access the unpinPage function in bufmgr package.
	   * @see bufmgr.unpinPage
	   */
	  private void unpinPage(PageId pageno, boolean dirty)
	    throws HFBufMgrException {

	    try {
	      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
	    }
	    catch (Exception e) {
	      throw new HFBufMgrException(e,"Heapfile.java: unpinPage() failed");
	    }

	  } // end of unpinPage


}
