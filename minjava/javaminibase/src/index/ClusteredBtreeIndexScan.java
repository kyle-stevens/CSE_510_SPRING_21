package index;

import btree.*;
import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Index Scan iterator will directly access the required tuple using
 * the provided key. Information about the tuples and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the tuples.
 */
public class ClusteredBtreeIndexScan extends Iterator {

  private final IndexFile indFile;
  private final IndexFileScan indScan;
  private final AttrType[] _types;
  private final short[] _s_sizes;
  public FldSpec[] perm_mat;
  private HFPage curr_page = null;
  private RID curr_page_RID = null;
  private CondExpr[] _selects;
  private List<Tuple> sortedBuffer;
  private int indexField, bufferPosition=0, bufferSize;
  private boolean buffered = false;

  /**
   * class constructor. set up the index scan.
   *
   * @param indName   name of the input index
   * @param types     array of types in this relation
   * @param str_sizes array of string sizes (for attributes that are string)
   * @param selects   conditions to apply, first one is primary
   * @throws IndexException error from the lower layer
   */
  public ClusteredBtreeIndexScan(
          final String indName,
          AttrType[] types,
          short[] str_sizes,
          CondExpr[] selects,
          int indexField
  )
          throws IndexException {
    _types = types;
    _s_sizes = str_sizes;
    _selects = selects;
    sortedBuffer = new ArrayList<>();
    this.indexField = indexField;

    try {
      indFile = new BTreeFile(indName);
    } catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
    }

    try {
      indScan = IndexUtils.BTree_scan(selects, indFile);
    } catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
    }
  }

  /**
   * returns the next tuple.
   *
   * @return the tuple
   * @throws IndexException          error from the lower layer
   * @throws UnknownKeyTypeException key type unknown
   * @throws IOException             from the lower layer
   */
  public Tuple get_next()
          throws IndexException,
          UnknownKeyTypeException,
          IOException, ScanIteratorException, HFBufMgrException, InvalidSlotNumberException,
          InvalidTupleSizeException, InvalidTypeException, UnknowAttrType, FieldNumberOutOfBoundException, PredEvalException {
    Tuple t;
    if (buffered) {
      if(bufferPosition >= bufferSize){
        buffered = false;
        sortedBuffer.clear();
        return get_next();
      }
      return sortedBuffer.get(bufferPosition++);
    } else {
      RID rid;
      KeyDataEntry nextEntry;
      nextEntry = ((BTFileScan)indScan).get_next_clustered_page();
      if (nextEntry == null) {
        return null;
      }
      rid = ((LeafData) nextEntry.data).getData();
      PageId pid = rid.pageNo;
      Page page = new Page();
      pinPage(pid, page);
      curr_page = new HFPage(page);
      curr_page_RID = curr_page.firstRecord();
      while(curr_page_RID != null){
        t = curr_page.getRecord(curr_page_RID);
        t.setHdr((short) _types.length, _types, _s_sizes);
        if(PredEval.Eval(_selects, t, null, _types, null)){
          sortedBuffer.add(t);
        }
        curr_page_RID = curr_page.nextRecord(curr_page_RID);
      }
      unpinPage(curr_page.getCurPage());
      sortedBuffer.sort((o1, o2) -> {
        int result = 0;
        try {
          switch (_types[indexField-1].attrType){
            case AttrType.attrString:
                result = o1.getStrFld(indexField).compareTo(o2.getStrFld(indexField));
                break;
            case AttrType.attrInteger:
                result = o1.getIntFld(indexField) - o2.getIntFld(indexField);
                break;
            case AttrType.attrReal:
                float f = o1.getFloFld(indexField) - o2.getFloFld(indexField);
                if(f>0) result = 1;
                if(f<0) result = -1;
                break;
          }
        } catch (IOException | FieldNumberOutOfBoundException e) {
          e.printStackTrace();
        }
        return result;
      });
      buffered = true;
      bufferPosition = 0;
      bufferSize = sortedBuffer.size();
      return get_next();
    }
  }

  /**
   * Cleaning up the index scan, does not remove either the original
   * relation or the index from the database.
   *
   * @throws IndexException error from the lower layer
   * @throws IOException    from the lower layer
   */
  public void close() throws IOException, IndexException {
    if (!closeFlag) {
      if (indScan instanceof BTFileScan) {
        try {
          ((BTreeFile) indFile).close();
          ((BTFileScan) indScan).DestroyBTreeFileScan();
        } catch (Exception e) {
          throw new IndexException(e, "BTree error in destroying index scan.");
        }
      }
      closeFlag = true;
    }
  }

  /**
   * short cut to access the pinPage function in bufmgr package.
   *
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageNo, Page page)
          throws HFBufMgrException {
    try {
      SystemDefs.JavabaseBM.pinPage(pageNo, page, false);
    } catch (Exception e) {
      throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
    }
  }

  /**
   * short cut to access the unpinPage function in bufmgr package.
   *
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageNo)
          throws HFBufMgrException {
    try {
      SystemDefs.JavabaseBM.unpinPage(pageNo, false);
    } catch (Exception e) {
      throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
    }
  }
}

