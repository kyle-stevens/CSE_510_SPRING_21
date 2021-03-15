package ui;

import btree.BTreeFile;
import btree.RealKey;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class Client {

  static final String relationName = "relation_name.in";
  public static boolean flag = false;
  static int n_pages;
  static int[] pref_list;
  static int pref_list_length;
  static AttrType[] _in;
  static FldSpec[] projection;

  public static void main(String args[]) {
    Scanner in = new Scanner(System.in);
    try {
      // Take the value of n_pages as user input
      System.out.println("Please input no of pages that can be used: ");
      n_pages = in.nextInt();
      try {
				// Throw exception if n_pages exceeds NUMBUF
        if (n_pages > GlobalConst.NUMBUF) {
          throw new Exception("n_pages exceeded the buffer size");
        }

        // Input the number of Skyline attributes
        System.out.println("Please enter the count of preference attributes: ");
        pref_list_length = in.nextInt();

        // Input Skyline Attributes
        System.out.println("Please enter numbers of columns, starting from 1 and separated by space, that could be used as preference list: ");
        pref_list = new int[pref_list_length];
        for (int i = 0; i < pref_list_length; i++) pref_list[i] = in.nextInt();

        // NestedLoopSky
        System.out.println("performNestedLoopSky START::");
				int temp = n_pages;
				//Reserving 6 pages for fileScans, heapfiles, etc. will be used by nestedLoopSkyline
				n_pages = 6;
        setupDB();
				//subtract 6 reserved pages from n_pages limit
				n_pages = temp - 6;
        try {
					//Passing remaining buffer pages that the NestedLoopsSky operator might use to store tuples.
					performNestedLoopsSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
        } catch (Exception e) {
          e.printStackTrace();
        }
				//restore n_pages limit for other operators to use
				n_pages = temp;
        System.out.println("performNestedLoopSky END::");

        // BlockNestedLoopSky
        System.out.println("performBlockNestedLoopSky START::");
        setupDB();
        try {
          performBlockNestedSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.out.println("performBlockNestedLoopSky END::");

        // SortFirstSky
        System.out.println("performSortFirstSky START::");
        setupDB();
        try {
          performSortedSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.out.println("performSortFirstSky END::");

        // BTreeSky
        System.out.println("performBTreeSky START::");
        setupDB();
        try {
          performBtreeSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.out.println("performBTreeSky END::");

        // BTreeSortedSky
        System.out.println("performBtreeSortedSky START::");
        setupDB();
        try {
          performBtreeSortedSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.out.println("performBtreeSortedSky END::");

      } catch (Exception e) {
        e.printStackTrace();
      }
      in.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

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

    new SystemDefs(dbpath, 100000, n_pages, "Clock");

    // Enter the path for data file
    File file = new File("/afs/asu.edu/users/s/p/a/spatil23/CSE510/data.txt");
    BufferedReader br = new BufferedReader(new FileReader(file));
    int numberOfCols = Integer.parseInt(br.readLine().trim());

    String str = "";

    _in = new AttrType[numberOfCols];
    for (int i = 0; i < numberOfCols; i++)
      _in[i] = new AttrType(AttrType.attrReal);

    projection = new FldSpec[numberOfCols];

    for (int i = 0; i < numberOfCols; i++) {
      projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
    }

    Tuple t = new Tuple();
    try {
      t.setHdr((short) numberOfCols, _in, null);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      e.printStackTrace();
    }

    int size = t.size();

    // Create heapfile and add the tuples to it
    Heapfile f = null;
    try {
      f = new Heapfile(relationName);
    } catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      e.printStackTrace();
    }

    t = new Tuple(size);
    try {
      t.setHdr((short) numberOfCols, _in, null);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      e.printStackTrace();
    }
    while ((str = br.readLine()) != null) {
      String attrs[] = str.split("\\t");


      int k = 1;

      for (String attr : attrs) {
        attr = attr.trim();
        if (attr.equals("")) continue;
        t.setFloFld(k++, Float.parseFloat(attr));
      }
      try {
        f.insertRecord(t.returnTupleByteArray());
      } catch (Exception e) {
        System.err.println("*** error in Heapfile.insertRecord() ***");
        e.printStackTrace();
        break;
      }
    }
    br.close();
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
																		String relationName, int n_pages) {
  	//For scanning outer loop
    FileScan nlScan = null;
    try {
      nlScan = new FileScan(relationName, in, Ssizes,
              (short) in.length, (short) in.length,
              projection, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
    PCounter.initialize();
    try {
      NestedLoopsSky nlSky = new NestedLoopsSky(in, in.length, Ssizes,
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
      nlSky.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    // Print the read / write count on disk
    printDiskAccesses();
  }

  // Start BlockNestedLoopSky
  static void performBlockNestedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list, int pref_list_length,
                                    String relationName, int n_pages) {
    FileScan am2 = null;
//     try {
//       am2 = new FileScan(relationName, in, Ssizes,
//               (short) in.length, (short) in.length,
//               projection, null);
//     } catch (Exception e) {
//       e.printStackTrace();
//     }
    BlockNestedLoopSky sky2 = null;
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
    if (n_pages < 6) throw new Exception("Not enough pages to create index");
    // create the index file
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
    scan.closescan();
    PCounter.initialize();
    Iterator sc = new BTreeSortedSky(in, (short) in.length, Ssizes, null,
            relationName, pref_list, pref_list_length, "BTreeIndex", n_pages - 1);
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

    Iterator am1 = new FileScan(relationName, in, Ssizes, (short) in.length, in.length, projection, null);
    PCounter.initialize();
    Iterator sc = new SortFirstSky(in, (short) in.length, Ssizes, am1, relationName, pref_list, pref_list_length, n_pages - 2);

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
//checking page limits for computation	  
    if (n_pages < (6+5*pref_list_length)) throw new Exception("Not enough pages to create files and perform passes");
//Storage of index file names	  
    String[] indexFiles = new String[pref_list_length];
//Create BTreeFiles and assign names to indexFiles[]	  
    for (int i = 0; i < pref_list_length; i++) {
      BTreeFile btf = null;
      try {
        indexFiles[i] = "BTreeIndex" + i;
        btf = new BTreeFile("BTreeIndex" + i, AttrType.attrReal, 4, 1);
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
          btf.insert(new RealKey(key), rid);
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
// close the file scan
      scan.closescan();
    }
//Set up PCounter	  
    PCounter.initialize();
//Create BTreeSky iterator and store tuples to buffer
//Reserves certain pages for file creation and scanners	  
    BTreeSky btScan = new BTreeSky(in, (short) in.length, Ssizes, null, relationName, pref_list, pref_list_length, indexFiles, n_pages - (6 + 5 * pref_list_length));

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
