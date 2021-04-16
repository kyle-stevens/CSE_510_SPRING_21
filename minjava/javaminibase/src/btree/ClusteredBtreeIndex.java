package btree;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.*;
import heap.*;
import index.IndexUtils;
import iterator.*;
import ui.Client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class ClusteredBtreeIndex {

  private static final String STR = "STR", INT = "INT", tempFileName = "tempDataFile";
  static FldSpec[] projection;
  private final BTreeFile bTreeFile;
  private final Heapfile relation;
  private final int indexField;
  private final String relationName, indexFileName;
  private int numFlds, keySize, recSize;
  private AttrType[] attrTypes;
  private AttrType indexAttrType;
  private short[] strSizes;

  public ClusteredBtreeIndex(String relationName, String filePath, String indexFileName,
                             int indexAttr) throws Exception {
    this.relationName = relationName;
    this.indexFileName = indexFileName;
    this.indexField = indexAttr;

    relation = new Heapfile(relationName, true);
    readDataFromFile(filePath);
    bTreeFile = new BTreeFile(indexFileName, indexAttrType.attrType, keySize, 1);
    clusterData();
  }

  public ClusteredBtreeIndex(String relationName, String indexFileName, int indexAttr,
                             AttrType[] attrTypes, short[] strSizes) throws Exception {
    this.relationName = relationName;
    this.indexFileName = indexFileName;
    this.indexField = indexAttr;
    this.numFlds = attrTypes.length;
    this.indexAttrType = attrTypes[indexAttr - 1];
    this.attrTypes = attrTypes;
    this.strSizes = strSizes;
    setKeySize();
    this.recSize = calculateRecSize(numFlds, attrTypes, strSizes);
    this.relation = new Heapfile(relationName, true);
    this.bTreeFile = new BTreeFile(indexFileName, indexAttrType.attrType, keySize, 1);
  }

  public short[] getStrSizes() {
    return strSizes;
  }

  public AttrType[] getAttrTypes() {
    return attrTypes;
  }

  public int getNumFlds() {
    return numFlds;
  }

  public void printCBtree() {
    try {
      BT.printBTree(bTreeFile.getHeaderPage());
      BT.printAllLeafPages(bTreeFile.getHeaderPage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private KeyClass getKeyClass(Tuple tuple) throws IOException, FieldNumberOutOfBoundException {
    switch (indexAttrType.attrType) {
      case 0:
        return new StringKey(tuple.getStrFld(indexField));
      case 1:
        return new IntegerKey(tuple.getIntFld(indexField));
      case 2:
        return new RealKey(tuple.getFloFld(indexField));
      default:
        System.out.println("invalid attribute type for index key");
        return null;
    }
  }

  private void readDataFromFile(String filePath) throws Exception {
    File file = new File(filePath);
    BufferedReader br = new BufferedReader(new FileReader(file));
    int numberOfCols = (short) Integer.parseInt(br.readLine().trim());
    numFlds = numberOfCols;

    if (indexField > numberOfCols || indexField < 1) {
      br.close();
      throw new Exception("Clustered Attribute is out of the range");
    }

    String str;

    attrTypes = new AttrType[numberOfCols];
    int numStr = 0;
    for (int i = 0; i < numberOfCols; i++) {
      str = br.readLine();
      String[] attrInfo = str.split("\\s+");
      if (attrInfo[1].equalsIgnoreCase(STR)) {
        numStr++;
        attrTypes[i] = new AttrType(AttrType.attrString);
      } else if (attrInfo[1].equalsIgnoreCase(INT)) {
        attrTypes[i] = new AttrType(AttrType.attrInteger);
      } else {
        attrTypes[i] = new AttrType(AttrType.attrReal);
      }
    }

    indexAttrType = attrTypes[indexField - 1];

    projection = new FldSpec[numberOfCols];

    for (int i = 0; i < numberOfCols; i++) {
      projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
    }

    strSizes = new short[numStr];
    Arrays.fill(strSizes, (short) 30);

    setKeySize();
    recSize = calculateRecSize(numberOfCols, attrTypes, strSizes);

    Tuple t = new Tuple(recSize);
    t.setHdr((short) numberOfCols, attrTypes, strSizes);

    Heapfile temp = new Heapfile(tempFileName);

    while ((str = br.readLine()) != null) {
      String[] attrs = str.split("\\s+");
      int k = 1;

      for (String attr : attrs) {
        attr = attr.trim();
        if (attr.equals(""))
          continue;
        switch (attrTypes[k - 1].attrType) {
          case AttrType.attrInteger:
            t.setIntFld(k++, Integer.parseInt(attr));
            break;
          case AttrType.attrString:
            t.setStrFld(k++, attr);
            break;
          case AttrType.attrReal:
            t.setFloFld(k++, Float.parseFloat(attr));
            break;
          default:
            break;
        }
      }

      temp.insertRecord(t.returnTupleByteArray());
    }
    br.close();
  }

  private void setKeySize() {
    switch (indexAttrType.attrType) {
      case 0:
        keySize = strSizes[0] * 2;
        break;
      case 1:
      case 2:
        keySize = 4;
        break;
      default:
        System.out.println("ERROR while setting key size: invalid attribute type for index key");
        break;
    }
  }

  private int calculateRecSize(int numberOfCols, AttrType[] attrTypes, short[] strSizes) {
    Tuple t = new Tuple();
    try {
      t.setHdr((short) numberOfCols, attrTypes, strSizes);
    } catch (IOException | InvalidTypeException | InvalidTupleSizeException e) {
      e.printStackTrace();
    }
    return t.size();
  }

  private void clusterData() throws Exception {
    FileScan tempScan = new FileScan(tempFileName, attrTypes, strSizes,
            (short) attrTypes.length, (short) attrTypes.length,
            projection, null);

    Sort sort = new Sort(attrTypes, (short) attrTypes.length, strSizes, tempScan,
            indexField, new TupleOrder(TupleOrder.Ascending), keySize, 200);

    int maxPageCapacity = (GlobalConst.MINIBASE_PAGESIZE * GlobalConst.MAX_PAGE_UTILIZATION) / (100 * (recSize + 4));

    Tuple next_tuple = sort.get_next();

    int tupleCount;
    RID keyTupleRid = null;
    Tuple lastTuple = next_tuple;
    for (tupleCount = 1; next_tuple != null; next_tuple = sort.get_next(), tupleCount++) {
      keyTupleRid = relation.insertRecord(next_tuple.returnTupleByteArray(), maxPageCapacity);
      if (tupleCount % maxPageCapacity == 0) {
        bTreeFile.insert(getKeyClass(next_tuple), keyTupleRid);
      }
      lastTuple = next_tuple;
    }
    tupleCount--;
    if (tupleCount % maxPageCapacity != 0) {
      bTreeFile.insert(getKeyClass(lastTuple), keyTupleRid);
    }
    sort.close();
    tempScan.close();
  }

  public RID insert(Tuple tuple) throws Exception {

    IndexFileScan indScan = IndexUtils.BTree_scan(getMatchCondition(tuple), bTreeFile);
    RID rid;
    KeyDataEntry nextEntry, lastEntry = null;
    boolean fisrtEntry = true;

    while (true) {
      nextEntry = ((BTFileScan) indScan).get_next_clustered_page();
      if (nextEntry == null) {
        //the given tuple's key is higher than all other existing keys
        break;
      }
      //there is an existing leaf page where new tuple can be inserted
      rid = ((LeafData) nextEntry.data).getData();
      PageId pid = rid.pageNo;
      Page page = new Page();
      pinPage(pid, page);
      HFPage curr_page = new HFPage(page);
      if (curr_page.available_space() >= recSize) {
        RID insertedRID = relation.insertRecord(tuple.getTupleByteArray(), pid);
        unpinPage(pid);
        ((BTFileScan) indScan).DestroyBTreeFileScan();
        return insertedRID;
      }
      lastEntry = nextEntry;
      fisrtEntry = false;
      unpinPage(pid);
    }
    ((BTFileScan) indScan).DestroyBTreeFileScan();

    //if there are no existing clusters/keys which can accommodate new tuple
    if (fisrtEntry) {
      rid = new RID();
      BTLeafPage lastLeafPage = bTreeFile.findRunEnd(null, rid);
      KeyDataEntry entry = lastLeafPage.getCurrent(rid);
      rid = ((LeafData) entry.data).getData();
      PageId pid = rid.pageNo;
      Page page = new Page();
      pinPage(pid, page);
      HFPage curr_page = new HFPage(page);
      if (curr_page.available_space() >= recSize) {
        //we will append this tuple to the last page of the data file
        RID newRid = relation.insertRecord(tuple.getTupleByteArray(), pid);
        //update the btree accordingly
        bTreeFile.Delete(entry.key, rid);
        bTreeFile.insert(getKeyClass(tuple), newRid);
        unpinPage(pid);
        return newRid;
      }
      unpinPage(pid);
      //if last page is full, add a new page
      rid = relation.insertRecordOnNewPage(tuple.returnTupleByteArray());
      //add entry to btree
      bTreeFile.insert(getKeyClass(tuple), rid);
      return rid;
    }

    //if matching pages are full, split them
    splitDataPage(lastEntry);
    return insert(tuple);
  }

  public List<RID> delete(Tuple tuple) throws Exception {
    CondExpr[] equalitySearch = getMatchCondition(tuple);
    IndexFileScan indScan = IndexUtils.BTree_scan(equalitySearch, bTreeFile);
    RID rid;
    KeyDataEntry nextEntry;
    List<RID> deletedRIDs = new ArrayList<>();

    while (true) {
      nextEntry = ((BTFileScan) indScan).get_next_clustered_page();
      if (nextEntry == null) {
        //the given tuple doesn't exist
        break;
      }
      //there is an existing leaf page where this tuple might be present
      rid = ((LeafData) nextEntry.data).getData();
      PageId pid = rid.pageNo;
      Page page = new Page();
      pinPage(pid, page);
      HFPage curr_page = new HFPage(page);

      RID curr_page_RID = curr_page.firstRecord();
      Tuple t;
      Map<Tuple, RID> sortedBuffer = getTreeMap();
      boolean keyDeleted = false;
      while(curr_page_RID != null){
        t = curr_page.getRecord(curr_page_RID);
        t.setHdr((short) numFlds, attrTypes, strSizes);
        if(TupleUtils.Equal(tuple, t, attrTypes, numFlds)) {
          if(curr_page_RID.equals(rid)) {
            bTreeFile.Delete(nextEntry.key, rid);
            keyDeleted = true;
          }
          relation.deleteRecord(curr_page_RID);
          deletedRIDs.add(curr_page_RID);
        } else {
          sortedBuffer.put(t, curr_page_RID);
        }
        curr_page_RID = curr_page.nextRecord(curr_page_RID);
      }

      List<Map.Entry<Tuple, RID>> sortedList = new ArrayList<>(sortedBuffer.entrySet());

      //if data page is not empty
      if(sortedList.size() > 0) {
        //if existing key in btree is deleted
        if(keyDeleted) {
          bTreeFile.insert(getKeyClass(sortedList.get(sortedList.size()-1).getKey()),
                  sortedList.get(sortedList.size()-1).getValue());
        }
        //unpin only if page is not empty
        unpinPage(curr_page.getCurPage());
      }
    }
    ((BTFileScan) indScan).DestroyBTreeFileScan();

    return deletedRIDs;
  }

  private void splitDataPage(KeyDataEntry entry) throws Exception {
    List<RID> oldRids = new ArrayList<>();
    List<RID> newRids = new ArrayList<>();
    RID rid = ((LeafData) entry.data).getData();
    PageId pid = rid.pageNo;
    Page page = new Page();
    pinPage(pid, page);
    HFPage curr_page = new HFPage(page);

    RID curr_page_RID = curr_page.firstRecord();
    Map<Tuple, RID> sortedBuffer = getTreeMap();
    Tuple t;
    while (curr_page_RID != null) {
      t = curr_page.getRecord(curr_page_RID);
      t.setHdr((short) attrTypes.length, attrTypes, strSizes);
      sortedBuffer.put(t, curr_page_RID);
      curr_page_RID = curr_page.nextRecord(curr_page_RID);
    }
    unpinPage(curr_page.getCurPage());
    int tupleCount = sortedBuffer.size();
    int splitPoint = tupleCount / 2;

    List<Map.Entry<Tuple, RID>> sortedList = new ArrayList<>(sortedBuffer.entrySet());
    Map.Entry<Tuple, RID> tupleToMove = null;
    RID newRid = null;
    PageId newPageid = null;

    for (int i = splitPoint; i < tupleCount; i++) {
      tupleToMove = sortedList.get(i);
      relation.deleteRecord(tupleToMove.getValue());
      if (tupleToMove.getValue().equals(rid)) {
        bTreeFile.Delete(getKeyClass(tupleToMove.getKey()), tupleToMove.getValue());
        bTreeFile.insert(getKeyClass(sortedList.get(splitPoint - 1).getKey()),
                sortedList.get(splitPoint - 1).getValue());
      }
      if (i == splitPoint) {
        newRid = relation.insertRecordOnNewPage(tupleToMove.getKey().returnTupleByteArray());
        newPageid = newRid.pageNo;
      } else {
        newRid = relation.insertRecord(tupleToMove.getKey().returnTupleByteArray(), newPageid);
      }
      oldRids.add(tupleToMove.getValue());
      newRids.add(newRid);
    }
    bTreeFile.insert(getKeyClass(tupleToMove.getKey()), newRid);
    //static function: to be implemented in ui/Client.java
    Client.reIndex(relationName, oldRids, newRids);
  }

  private CondExpr[] getMatchCondition(Tuple tuple) throws FieldNumberOutOfBoundException, IOException {
    CondExpr[] expr = new CondExpr[2];
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(indexAttrType.attrType);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), indexField);
    switch (indexAttrType.attrType) {
      case AttrType.attrInteger:
        expr[0].operand2.integer = tuple.getIntFld(indexField);
        break;
      case AttrType.attrString:
        expr[0].operand2.string = tuple.getStrFld(indexField);
        break;
      case AttrType.attrReal:
        expr[0].operand2.real = tuple.getFloFld(indexField);
        break;
    }
    expr[0].next = null;
    return expr;
  }

  private Map<Tuple, RID> getTreeMap() {
    return new TreeMap<>((o1, o2) -> {
      int result = 0;
      try {
        switch (attrTypes[indexField - 1].attrType) {
          case AttrType.attrString:
            result = o1.getStrFld(indexField).compareTo(o2.getStrFld(indexField));
            break;
          case AttrType.attrInteger:
            result = o1.getIntFld(indexField) - o2.getIntFld(indexField);
            break;
          case AttrType.attrReal:
            float f = o1.getFloFld(indexField) - o2.getFloFld(indexField);
            if (f > 0) result = 1;
            if (f < 0) result = -1;
            break;
        }
      } catch (IOException | FieldNumberOutOfBoundException e) {
        e.printStackTrace();
      }
      return result;
    });
  }

  public void close() {
    try {
      bTreeFile.close();
    } catch (PageUnpinnedException | InvalidFrameNumberException | HashEntryNotFoundException | ReplacerException e) {
      e.printStackTrace();
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
