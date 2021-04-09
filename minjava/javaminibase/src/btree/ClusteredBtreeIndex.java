package btree;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Tuple;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class ClusteredBtreeIndex {

  private static final String STR = "STR", INT = "INT", tempFileName = "tempDataFile";
  static FldSpec[] projection;
  private final BTreeFile bTreeFile;
  private final Heapfile relation;
  private final int indexField;
  private final String fileName, indexFileName, filePath;
  private int numFlds, keySize, recSize;
  private AttrType[] attrTypes;
  private short[] strSizes;

  public ClusteredBtreeIndex(String fileName, String filePath, String indexFileName, int indexAttr) throws Exception {
    this.fileName = fileName;
    this.filePath = filePath;
    this.indexFileName = indexFileName;
    this.indexField = indexAttr;

    relation = new Heapfile(fileName, true);
    readData();
    bTreeFile = new BTreeFile(indexFileName, attrTypes[indexAttr - 1].attrType, keySize, 1);
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

    String str = "";

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
      String attrs[] = str.split("\\s+");
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
    switch (attrTypes[indexField - 1].attrType) {
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
        bTreeFile.insert(getKeyClass(next_tuple, indexField, attrTypes[indexField - 1]), keyTupleRid);
      }
      lastTuple = next_tuple;
    }
    tupleCount--;
    if (tupleCount % maxPageCapacity != 0) {
      bTreeFile.insert(getKeyClass(lastTuple, indexField, attrTypes[indexField - 1]), keyTupleRid);
    }
    sort.close();
    tempScan.close();
  }
}
