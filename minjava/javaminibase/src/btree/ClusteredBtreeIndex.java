package index;

import btree.BT;
import btree.BTreeFile;
import btree.RealKey;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import iterator.Iterator;
import iterator.Sort;

public class ClusteredBtreeIndex {

  private final BTreeFile bTreeFile;

  private final Heapfile sortedDataFile;

  public ClusteredBtreeIndex(String sortedDataFileName, String indexFileName, int keySize,
                             int delete_fashion, int recSize, Iterator scan,
                             AttrType attrTypes[], short[] str_sizes, int indexAttr,
                             int n_pages) throws Exception {

    bTreeFile = new BTreeFile(indexFileName, attrTypes[indexAttr].attrType, keySize, delete_fashion);

    Sort sort = new Sort(attrTypes, (short)attrTypes.length, str_sizes, scan,
            indexAttr, new TupleOrder(TupleOrder.Ascending), keySize, n_pages);

    sortedDataFile = new Heapfile(sortedDataFileName,true);

    int maxPageCapacity = (GlobalConst.MINIBASE_PAGESIZE * GlobalConst.MAX_PAGE_UTILIZATION)/(100* (recSize+4));

    Tuple next_tuple = sort.get_next();

    for(int tupleCount = 1; next_tuple != null; next_tuple = sort.get_next(), tupleCount++) {
      RID keyTupleRid = sortedDataFile.insertRecord(next_tuple.returnTupleByteArray(), maxPageCapacity);
      if (tupleCount % maxPageCapacity == 0) { //reason for this needs to be commented
        bTreeFile.insert(new RealKey(next_tuple.getFloFld(indexAttr)), keyTupleRid);
      }
    }
  }

  public void printCBtree() {
    try {
      BT.printBTree(bTreeFile.getHeaderPage());
      BT.printAllLeafPages(bTreeFile.getHeaderPage());
    }catch (Exception e) {
      e.printStackTrace();
    }
  }
}
