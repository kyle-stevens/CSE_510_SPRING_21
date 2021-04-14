package ui;

import btree.*;
import diskmgr.PCounter;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.ClusteredBtreeIndexScan;
import index.IndexException;
import iterator.*;

import java.io.IOException;

public class Client {

  static final String relationName = "relation_name.in";
  public static boolean flag = false;
  static int n_pages;
  static int[] pref_list;
  static int pref_list_length;
  static AttrType[] _in;
  static FldSpec[] projection;

  public static void main(String args[]) {
    try {
      setupDB();
      ClusteredBtreeIndex clusteredBtreeIndex = new ClusteredBtreeIndex("sample1",
              "/afs/asu.edu/users/s/p/a/spatil23/CSE510/minjava/javaminibase/src/sample.txt","btree", 1);
      System.out.println("**printing btree");
      clusteredBtreeIndex.printCBtree();
      Heapfile temp = new Heapfile("sample1");
      Scan sc = temp.openScan();
      RID t = new RID();
      Tuple tuple;
      System.out.println("Printing datafile sequentially");
      while((tuple = sc.getNext(t)) != null) {
        tuple.setHdr((short)clusteredBtreeIndex.getNumFlds(), clusteredBtreeIndex.getAttrTypes(), clusteredBtreeIndex.getStrSizes());
        tuple.print(clusteredBtreeIndex.getAttrTypes());
      }
      sc.closescan();

//      System.out.println("Testing range scan");
//      CondExpr[] expr = new CondExpr[3];
//      expr[0] = new CondExpr();
//      expr[0].op    = new AttrOperator(AttrOperator.aopGE);
//      expr[0].type1 = new AttrType(AttrType.attrSymbol);
//      expr[0].type2 = new AttrType(AttrType.attrInteger);
//      expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
//      expr[0].operand2.integer = 400;
//      expr[0].next = null;
//
//      expr[1] = new CondExpr();
//      expr[1].op    = new AttrOperator(AttrOperator.aopLE);
//      expr[1].next  = null;
//      expr[1].type1 = new AttrType(AttrType.attrSymbol);
//      expr[1].type2 = new AttrType(AttrType.attrInteger);
//      expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
//      expr[1].operand2.integer = 500;
//      expr[2] = null;
//
//      ClusteredBtreeIndexScan iscan = new ClusteredBtreeIndexScan("btree", clusteredBtreeIndex.getAttrTypes(),
//              clusteredBtreeIndex.getStrSizes(), expr, 2, false);
//      System.out.println("Scanning index");
//      while((tuple = iscan.get_next()) != null) {
//        tuple.setHdr((short)clusteredBtreeIndex.getNumFlds(), clusteredBtreeIndex.getAttrTypes(), clusteredBtreeIndex.getStrSizes());
//        tuple.print(clusteredBtreeIndex.getAttrTypes());
//      }
//      iscan.close();

      System.out.println("**printing btree");
      clusteredBtreeIndex.printCBtree();

      Tuple add = new Tuple();
      add.setHdr((short)clusteredBtreeIndex.getNumFlds(), clusteredBtreeIndex.getAttrTypes(), clusteredBtreeIndex.getStrSizes());
      add = new Tuple(add.size());
      add.setHdr((short)clusteredBtreeIndex.getNumFlds(), clusteredBtreeIndex.getAttrTypes(), clusteredBtreeIndex.getStrSizes());
      add.setStrFld(1, "ZZZZZZZZZZ");
      add.setIntFld(2,0);
      add.setIntFld(3,0);

      clusteredBtreeIndex.insert(add);

      ClusteredBtreeIndexScan iscan = new ClusteredBtreeIndexScan("btree", clusteredBtreeIndex.getAttrTypes(),
              clusteredBtreeIndex.getStrSizes(), null, 1, false);
      System.out.println("Scanning index");
      while((tuple = iscan.get_next()) != null) {
        tuple.setHdr((short)clusteredBtreeIndex.getNumFlds(), clusteredBtreeIndex.getAttrTypes(), clusteredBtreeIndex.getStrSizes());
        tuple.print(clusteredBtreeIndex.getAttrTypes());
      }
      iscan.close();

      add = new Tuple(add.size());
      add.setHdr((short)clusteredBtreeIndex.getNumFlds(), clusteredBtreeIndex.getAttrTypes(), clusteredBtreeIndex.getStrSizes());
      add.setStrFld(1, "AAA");
      add.setIntFld(2,99999);
      add.setIntFld(3,99999);

      clusteredBtreeIndex.insert(add);

      iscan = new ClusteredBtreeIndexScan("btree", clusteredBtreeIndex.getAttrTypes(),
              clusteredBtreeIndex.getStrSizes(), null, 1, false);
      System.out.println("Scanning index");
      while((tuple = iscan.get_next()) != null) {
        tuple.setHdr((short)clusteredBtreeIndex.getNumFlds(), clusteredBtreeIndex.getAttrTypes(), clusteredBtreeIndex.getStrSizes());
        tuple.print(clusteredBtreeIndex.getAttrTypes());
      }
      iscan.close();

      add = new Tuple(add.size());
      add.setHdr((short)clusteredBtreeIndex.getNumFlds(), clusteredBtreeIndex.getAttrTypes(), clusteredBtreeIndex.getStrSizes());
      add.setStrFld(1, "LLLLL");
      add.setIntFld(2,500);
      add.setIntFld(3,5000);

      clusteredBtreeIndex.insert(add);

      iscan = new ClusteredBtreeIndexScan("btree", clusteredBtreeIndex.getAttrTypes(),
              clusteredBtreeIndex.getStrSizes(), null, 1, true);
      System.out.println("Scanning index");
      while((tuple = iscan.get_reversed_next()) != null) {
        tuple.setHdr((short)clusteredBtreeIndex.getNumFlds(), clusteredBtreeIndex.getAttrTypes(), clusteredBtreeIndex.getStrSizes());
        tuple.print(clusteredBtreeIndex.getAttrTypes());
      }
      iscan.close();

//      clusteredBtreeIndex.close();
//      clusteredBtreeIndex.printCBtree();
//      KeyClass testKey = new IntegerKey(1500);
//      RID testRID = new RID();
//      BTLeafPage testPage = clusteredBtreeIndex.test(testKey, testRID);
//      KeyDataEntry testKeyDataEntry = testPage.getCurrent(testRID);
//      System.out.println("Key = " + ((IntegerKey)testKeyDataEntry.key).getKey().toString());
//
//      clusteredBtreeIndex.printCBtree();
//
//      testRID = new RID();
//      testPage = clusteredBtreeIndex.test(null, testRID);
//      testKeyDataEntry = testPage.getCurrent(testRID);
//      System.out.println("Key = " + ((IntegerKey)testKeyDataEntry.key).getKey().toString());
//
//      clusteredBtreeIndex.printCBtree();
//
//      testKey = new IntegerKey(4000);
//      testRID = new RID();
//      testPage = clusteredBtreeIndex.test(testKey, testRID);
//      testKeyDataEntry = testPage.getCurrent(testRID);
//      testKeyDataEntry = testPage.getPrev(testRID);
//      System.out.println("Key = " + ((IntegerKey)testKeyDataEntry.key).getKey().toString());

      clusteredBtreeIndex.printCBtree();
      clusteredBtreeIndex.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
static SystemDefs sys;
  // Setup database from text file
  static void setupDB() throws NumberFormatException, IOException, FieldNumberOutOfBoundException {
    String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.skylineDB";
    String logpath = "/tmp/" + System.getProperty("user.name") + ".skylog";

    String remove_cmd = "/bin/rm -rf ";
    String remove_logcmd = remove_cmd + logpath;
    String remove_dbcmd = remove_cmd + dbpath;
    String remove_joincmd = remove_cmd + dbpath;

    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
      Runtime.getRuntime().exec(remove_joincmd);
    } catch (IOException e) {
      System.err.println("" + e);
    }

    sys = new SystemDefs(dbpath, 100000, GlobalConst.NUMBUF, "Clock");

    // Enter the path for data file
//    File file = new File("/afs/asu.edu/users/s/p/a/spatil23/CSE510/pc_inc_2_7000.txt");
//    BufferedReader br = new BufferedReader(new FileReader(file));
//    int numberOfCols = Integer.parseInt(br.readLine().trim());
//
//    String str = "";
//
//    _in = new AttrType[numberOfCols];
//    for(int i = 0; i < numberOfCols; i++)
//      _in[i] = new AttrType(AttrType.attrReal);
//
//    projection = new FldSpec[numberOfCols];
//
//    for (int i = 0; i < numberOfCols; i++) {
//      projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
//    }
//
//    Tuple t = new Tuple();
//    try {
//      t.setHdr((short) numberOfCols, _in, null);
//    } catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      e.printStackTrace();
//    }
//
//    int size = t.size();
//
//    // Create heapfile and add the tuples to it
//    Heapfile f = null;
//    try {
//      f = new Heapfile(relationName);
//    } catch (Exception e) {
//      System.err.println("*** error in Heapfile constructor ***");
//      e.printStackTrace();
//    }
//
//    t = new Tuple(size);
//    try {
//      t.setHdr((short) numberOfCols, _in, null);
//    } catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      e.printStackTrace();
//    }
//    while ((str = br.readLine()) != null) {
//      String attrs[] = str.split("\\t");
//
//
//      int k = 1;
//
//      for (String attr : attrs) {
//        attr = attr.trim();
//        if (attr.equals("")) continue;
//        t.setFloFld(k++, Float.parseFloat(attr));
//      }
//      try {
//        f.insertRecord(t.returnTupleByteArray());
//      } catch (Exception e) {
//        System.err.println("*** error in Heapfile.insertRecord() ***");
//        e.printStackTrace();
//        break;
//      }
//    }
//    br.close();
  }

  // Print Skyline tuples
  static void printTuple(int tuple_count, Tuple t) throws Exception {
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    System.out.println("Skyline tuple # " + tuple_count);
    System.out.println("-----------------------------------");
    t.print(_in);
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
  }

  // Start NestedLoopsSky
  static void performNestedLoopsSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list, int pref_list_length,
																		String relationName, int n_pages) throws IndexException, IOException {
  	//For scanning outer loop
	  sys.flushBuffer();
	    PCounter.initialize();
    FileScan nlScan = null;
    try {
      nlScan = new FileScan(relationName, in, Ssizes,
              (short) in.length, (short) in.length,
              projection, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
    NestedLoopsSky nlSky = null;
    try {
      nlSky = new NestedLoopsSky(in, in.length, Ssizes,
              nlScan, relationName, pref_list, pref_list_length, n_pages);
      System.out.println("**************************************************");
      System.out.println("\t NESTED LOOP SKYLINE");
      System.out.println("**************************************************");

      // Print skyline tuples
      Tuple nestedLoopSkyline;
      int tuple_count = 1;
      while ((nestedLoopSkyline = nlSky.get_next()) != null) {
        printTuple(tuple_count, nestedLoopSkyline);
        tuple_count++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }finally {
    	if(nlScan!=null)
    	nlScan.close();
    	if(nlSky!=null)
        nlSky.close();
    }
    // Print the read / write count on disk
    printDiskAccesses();
  }

  // Start BlockNestedLoopSky
  static void performBlockNestedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list, int pref_list_length,
                                    String relationName, int n_pages) {
    FileScan am2 = null;
    BlockNestedLoopSky sky2 = null;	  
    sys.flushBuffer();

    PCounter.initialize();
    try {
      //flag = true;
      sky2 = new BlockNestedLoopSky(in, in.length, Ssizes,
              am2, relationName, pref_list, pref_list_length, n_pages);
      System.out.println("**************************************************");
      System.out.println("**************************************************");
      System.out.println("\t\tBLOCK NESTED SKYLINE ");
      System.out.println("**************************************************");
      System.out.println("**************************************************\n");

      // Print skyline tuples
      Tuple t1 = null;
      int tuple_count = 1;
      try {
        while ((t1 = sky2.get_next()) != null) {
          printTuple(tuple_count, t1);
          tuple_count++;
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        sky2.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    // Print the read / write count on disk
    printDiskAccesses();
  }

  // Start BtreeSortedSky
  static void performBtreeSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list, int pref_list_length,
                                    String relationName, int n_pages) throws Exception {

	    if (sys.getUnpinCount() < 6) throw new Exception("Not enough pages to create index");
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTreeIndex", AttrType.attrReal, 4, 1);
    } catch (Exception e) {
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    RID rid = new RID();
    float key = 0;
    Tuple temp = null;
    Scan scan = new Scan(new Heapfile(relationName));
    try {
      temp = scan.getNext(rid);
    } catch (Exception e) {
      e.printStackTrace();
    }

    Tuple tt = new Tuple();
    try {
      tt.setHdr((short) in.length, in, Ssizes);
    } catch (Exception e) {
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
      tt.setHdr((short) in.length, in, Ssizes);
    } catch (Exception e) {
      e.printStackTrace();
    }
    while (temp != null) {

      try {
        tt.tupleCopy(temp);
        float tmp = (float) TupleUtils.getPrefAttrSum(tt, in, (short) in.length, pref_list, pref_list_length);

        key = -tmp;
        //  System.out.println(key);
      } catch (Exception e) {
        e.printStackTrace();
      }

      try {
        btf.insert(new RealKey(key), rid);
      } catch (Exception e) {
        e.printStackTrace();
      }

      try {
        temp = scan.getNext(rid);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // close the file scan
    btf.close();
    
    scan.closescan();
	  sys.flushBuffer();

    PCounter.initialize();
    Iterator sc = new BTreeSortedSky(in, (short) in.length, Ssizes, null,
            relationName, pref_list, pref_list_length, "BTreeIndex", n_pages );
    Tuple t1 = null;
    int tuple_count = 1;
    try {
      while ((t1 = sc.get_next()) != null) {
        printTuple(tuple_count, t1);
        tuple_count++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      sc.close();
    }
    // Print the read / write count on disk
    printDiskAccesses();
  }

  // Start SortFistSky
  static void performSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list, int pref_list_length,
                               String relationName, int n_pages) throws Exception {
	  sys.flushBuffer();

	PCounter.initialize();
	
    Iterator am1 = new FileScan(relationName, in, Ssizes, (short) in.length, in.length, projection, null);
    Iterator sc = null;
    try {
    	sc = new SortFirstSky(in, (short) in.length, Ssizes, am1, relationName, pref_list, pref_list_length, n_pages - 2);
    }catch(Exception e) {
    	if(am1!=null)am1.close();
    	throw e;
    }
    // Print Skyline tuples
    Tuple t1 = null;
    int tuple_count = 1;
    try {
      while ((t1 = sc.get_next()) != null) {
        printTuple(tuple_count, t1);
        tuple_count++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      sc.close();
    }
    // Print the read / write count on disk
    printDiskAccesses();
  }

  // Start BtreeSky
  static void performBtreeSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list, int pref_list_length,
                              String relationName, int n_pages) throws Exception {
//////checking page limits for construction of index	  
    if (sys.getUnpinCount() < 6) throw new Exception("Not enough pages to create index");
//Storage of index file names	  
    String[] indexFiles = new String[pref_list_length];
    BTreeFile btf[] = new BTreeFile[pref_list_length];
//Create BTreeFiles and assign names to indexFiles[]	  
    for (int i = 0; i < pref_list_length; i++) {
      try {
        indexFiles[i] = "BTreeIndex" + i;
        btf[i] = new BTreeFile(indexFiles[i], AttrType.attrReal, 4, 1);
      } catch (Exception e) {
        e.printStackTrace();
        Runtime.getRuntime().exit(1);
      }
//Begin sorting process	    
      RID rid = new RID();
      float key = 0;
      Tuple temp = null;
      Scan scan = new Scan(new Heapfile(relationName));
      try {
        temp = scan.getNext(rid);
      } catch (Exception e) {
        e.printStackTrace();
      }

      Tuple tt = new Tuple();
      try {
        tt.setHdr((short) in.length, in, Ssizes);
      } catch (Exception e) {
        e.printStackTrace();
      }

      int sizett = tt.size();
      tt = new Tuple(sizett);
      try {
        tt.setHdr((short) in.length, in, Ssizes);
      } catch (Exception e) {
        e.printStackTrace();
      }
      while (temp != null) {

        try {
          tt.tupleCopy(temp);
          float tmp = tt.getFloFld(pref_list[i]);
//Use negative key to sort in reverse(descending) order
          key = -tmp;
          //  System.out.println(key);
        } catch (Exception e) {
          e.printStackTrace();
        }
//Add element to BTreeFile
        try {
          btf[i].insert(new RealKey(key), rid);
        } catch (Exception e) {
          e.printStackTrace();
        }
//Get next element to sort and add
        try {
          temp = scan.getNext(rid);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      btf[i].close();
// close the file scan
      scan.closescan();
    }
//Set up PCounter		  
    sys.flushBuffer();

    PCounter.initialize();
//Create BTreeSky iterator and store tuples to buffer
//Reserves certain pages for file creation and scanners	  

    BTreeSky btScan = new BTreeSky(in, (short) in.length, Ssizes, null, relationName, pref_list, pref_list_length, indexFiles, n_pages);

// Print Skyline Tuples
    Tuple t1 = null;
    int tuple_count = 1;
    try {
      while ((t1 = btScan.get_next()) != null) {
        printTuple(tuple_count, t1);
        tuple_count++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      btScan.close();
    }
// Print the read / write count on disk
    printDiskAccesses();

  }

  // Print read write on disk
  static void printDiskAccesses() {
    System.out.println("Read Count: " + PCounter.rcounter);

    System.out.println("Write Count: " + PCounter.wcounter);
  }
}
