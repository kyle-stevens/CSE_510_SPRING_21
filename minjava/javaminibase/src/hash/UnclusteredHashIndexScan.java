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
        keyFileAttr[1] = new AttrType(AttrType.attrInteger);
        
        if(_in[attr_no-1].attrType==AttrType.attrString) {
        	keyFileStrLens = new short[1];
        	keyFileStrLens[0] = sSizes[0];
        }		
		_keyPageAttr = new AttrType[2];
		_keyPageAttr[0] = new AttrType(AttrType.attrInteger);
		_keyPageAttr[1] = new AttrType(AttrType.attrInteger);
		
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
		
		if(curr_page!=null){
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
			t.setHdr((short)2,_keyPageAttr,null);
			t = new Tuple(t);
			t.setHdr((short)2,_keyPageAttr,null);
			t = relationFile.getRecord(ridFromTuple(t));
			t.setHdr((short)_in.length, _in, _sSizes);
			t = new Tuple(t);
			t.setHdr((short)_in.length, _in, _sSizes);
			return t;
		}else if(currBucketScan!=null) {
			Tuple keyPair = currBucketScan.getNext(new RID());
			if(keyPair==null) {
				currBucketScan.closescan();
				currBucketScan=null;
				return get_next();
			}
			keyPair.setHdr((short)2, keyFileAttr, keyFileStrLens);
			keyPair = new Tuple(keyPair);
			keyPair.setHdr((short)2, keyFileAttr, keyFileStrLens);
			PageId pid = new PageId(keyPair.getIntFld(2));
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
		if(currBucketScan!=null) {
			currBucketScan.closescan();
		}
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
