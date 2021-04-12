package btree;

import diskmgr.Page;
import global.*;
import heap.*;
import iterator.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ClusteredBtreeIndex {

  private static final String STR = "STR", INT = "INT", tempFileName = "tempDataFile";
  static FldSpec[] projection;
  private final BTreeFile bTreeFile;
  private final Heapfile relation;
  private final int indexField;
  private final String fileName, indexFileName, filePath;
  private int numFlds, keySize, recSize;
  private AttrType[] attrTypes;
  private AttrType indexType;
  private short[] strSizes;

  public ClusteredBtreeIndex(String fileName, String filePath, String indexFileName, int indexAttr) throws Exception {
    this.fileName = fileName;
    this.filePath = filePath;
    this.indexFileName = indexFileName;
    this.indexField = indexAttr;

    relation = new Heapfile(fileName, true);
    readData();
    bTreeFile = new BTreeFile(indexFileName, indexType.attrType, keySize, 1);
    clusterData();
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

  private KeyClass getKeyClass(Tuple tuple, int attrIndex, AttrType type) throws IOException, FieldNumberOutOfBoundException {
    switch (type.attrType) {
      case 0:
        return new StringKey(tuple.getStrFld(attrIndex));
      case 1:
        return new IntegerKey(tuple.getIntFld(attrIndex));
      case 2:
        return new RealKey(tuple.getFloFld(attrIndex));
      default:
        System.out.println("invalid attribute type for index key");
        return null;
    }
  }

  private void readData() throws Exception {
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

    indexType = attrTypes[indexField-1];

    projection = new FldSpec[numberOfCols];

    for (int i = 0; i < numberOfCols; i++) {
      projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
    }

    strSizes = new short[numStr];
    Arrays.fill(strSizes, (short) 30);

    setKeySize();

    Tuple t = new Tuple();
    t.setHdr((short) numberOfCols, attrTypes, strSizes);
    recSize = t.size();

    t = new Tuple(recSize);
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
    switch (indexType.attrType) {
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
        bTreeFile.insert(getKeyClass(next_tuple, indexField, indexType), keyTupleRid);
      }
      lastTuple = next_tuple;
    }
    tupleCount--;
    if (tupleCount % maxPageCapacity != 0) {
      bTreeFile.insert(getKeyClass(lastTuple, indexField, indexType), keyTupleRid);
    }
    sort.close();
    tempScan.close();
  }

  //Might remove later - not used currently
  public Tuple lookUp(String key) throws Exception {
    if(indexType.attrType != 0){
      throw new Exception("Invalid key type");
    }
    PageId dataPageId = bTreeFile.getDataPageID(new StringKey(key));
    if(dataPageId == null) {
      return null;
    }
    Page page = new Page();
    pinPage(dataPageId, page);
    HFPage curr_page = new HFPage(page);
    RID curr_page_RID = null;
    Tuple match;
    do {
      if (curr_page_RID != null) {
        curr_page_RID = curr_page.nextRecord(curr_page_RID);
      } else {
        curr_page_RID = curr_page.firstRecord();
      }
      if (curr_page_RID == null) {
        unpinPage(curr_page.getCurPage());
        return null;
      }
      match = curr_page.getRecord(curr_page_RID);
      match.setHdr((short) attrTypes.length, attrTypes, strSizes);
      if(key.equals(match.getStrFld(indexField)))
        return match;
    }while (true);
  }

  //Might remove later - not used currently
  public Tuple lookUp(int key) throws Exception {
    if(indexType.attrType != 1){
      throw new Exception("Invalid key type");
    }
    PageId dataPageId = bTreeFile.getDataPageID(new IntegerKey(key));
    if(dataPageId == null) {
      return null;
    }
    Page page = new Page();
    pinPage(dataPageId, page);
    HFPage curr_page = new HFPage(page);
    RID curr_page_RID = null;
    Tuple match;
    do {
      if (curr_page_RID != null) {
        curr_page_RID = curr_page.nextRecord(curr_page_RID);
      } else {
        curr_page_RID = curr_page.firstRecord();
      }
      if (curr_page_RID == null) {
        unpinPage(curr_page.getCurPage());
        return null;
      }
      match = curr_page.getRecord(curr_page_RID);
      match.setHdr((short) attrTypes.length, attrTypes, strSizes);
      if(key == match.getIntFld(indexField))
        return match;
    }while (true);
  }

  //Might remove later - not used currently
  public Tuple lookUp(float key) throws Exception {
    if(indexType.attrType != 2){
      throw new Exception("Invalid key type");
    }
    PageId dataPageId = bTreeFile.getDataPageID(new RealKey(key));
    if(dataPageId == null) {
      return null;
    }
    Page page = new Page();
    pinPage(dataPageId, page);
    HFPage curr_page = new HFPage(page);
    RID curr_page_RID = null;
    Tuple match;
    do {
      if (curr_page_RID != null) {
        curr_page_RID = curr_page.nextRecord(curr_page_RID);
      } else {
        curr_page_RID = curr_page.firstRecord();
      }
      if (curr_page_RID == null) {
        unpinPage(curr_page.getCurPage());
        return null;
      }
      match = curr_page.getRecord(curr_page_RID);
      match.setHdr((short) attrTypes.length, attrTypes, strSizes);
      if(key == match.getFloFld(indexField))
        return match;
    }while (true);
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
