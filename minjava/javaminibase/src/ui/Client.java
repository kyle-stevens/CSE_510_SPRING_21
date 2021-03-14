package ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;

import btree.BTreeFile;
import btree.RealKey;
import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.*;

public class Client {

	static int n_pages;
	static int[] pref_list;
	static int pref_list_length;
	static AttrType[] _in;
	static FldSpec[] projection;
	static final String relationName = "relation_name.in";
	public static boolean flag = false;
	public static void main(String args[]) {
		Scanner in = new Scanner(System.in);
		try {
			System.out.println("Please input no of pages that can be used: ");
			n_pages = in.nextInt();
			System.out.println("Please enter the count of preference attributes: ");
			pref_list_length = in.nextInt();
			System.out.println("Please enter numbers of columns, starting from 1 and separated by space, that could be used as preference list: ");
			pref_list= new int[pref_list_length];
			for(int i=0;i<pref_list_length;i++)pref_list[i] = in.nextInt();

			System.out.println("performNestedLoopSkyNaive START::");
			setupDB();
			try {
				performNestedLoopsSkyNaive(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("performNestedLoopSkyNaive END::");

			setupDB();
			try {
				performBlockNestedSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("performBlockNestedSky END::");
			
			System.out.println("performSortedSky START::");
			setupDB();
			try {
				performSortedSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("performSortedSky END::");
			
			System.out.println("performBtreeSortedSky START::");
			setupDB();
			try {
				performBtreeSortedSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("performBtreeSortedSky END::");
			
			System.out.println("performBTreeSky START::");
			setupDB();
			try {
				performNestedLoopsSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("performBTreeSky END::");

			System.out.println("performBlockNestedSky START::");
			
			System.out.println("performNestedLoopSky START::");
			setupDB();
			try {
				performNestedLoopsSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("performNestedLoopSky END::");

			System.out.println("performBlockNestedSky START::");
		} catch (Exception e) {
			e.printStackTrace();
		}
		in.close();
	}
	static void setupDB() throws NumberFormatException, IOException, FieldNumberOutOfBoundException {
		String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.skylineDB"; 
	    String logpath = "/tmp/"+System.getProperty("user.name")+".skylog";

	    String remove_cmd = "/bin/rm -rf ";
	    String remove_logcmd = remove_cmd + logpath;
	    String remove_dbcmd = remove_cmd + dbpath;
	    String remove_joincmd = remove_cmd + dbpath;

	    try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	      Runtime.getRuntime().exec(remove_joincmd);
	    }
	    catch (IOException e) {
	      System.err.println (""+e);
	    }

	    new SystemDefs( dbpath, 100000, n_pages, "Clock" );
	    
	    File file = new File("/afs/asu.edu/users/j/t/r/jtrada/data2.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		int numberOfCols = Integer.parseInt(br.readLine().trim());

		String str = "";

		_in = new AttrType[numberOfCols];
		for (int i = 0; i < numberOfCols; i++)
			_in[i] = new AttrType(AttrType.attrReal);
		
		projection = new FldSpec[numberOfCols];
		
		for(int i=0;i<numberOfCols;i++) {
			projection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
		}
		
		Tuple t = new Tuple();
		try {
			t.setHdr((short) numberOfCols, _in, null);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		int size = t.size();

		// inserting the tuple into file "sailors"
		Heapfile f = null;
		try {
			f = new Heapfile(relationName);
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}
		
		t = new Tuple(size);
		System.out.println(size);
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
				if(attr.equals("")) continue;
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
	static void performNestedLoopsSkyNaive(AttrType[] in, short[] Ssizes, FldSpec[] projection,int[] pref_list,int pref_list_length,
																				 String relationName, int n_pages) {
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
			//n_pages - 2 : since FileScan (nlScan) will use 2 buffer pages
			NestedLoopsSkyNaive nlSky = new NestedLoopsSkyNaive(in, in.length, Ssizes,
							nlScan, relationName, pref_list, pref_list_length, n_pages-2);
			System.out.println("**************************************************");
			System.out.println("\t NESTED LOOP SKYLINE (Naive approach) ");
			System.out.println("**************************************************");

			Tuple nestedLoopSkyline;
			int tuple_count = 1;
			while ((nestedLoopSkyline = nlSky.get_next()) != null) {
				printTuple(tuple_count,nestedLoopSkyline);
				tuple_count++;
			}
			nlSky.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		printDiskAccesses();
	}
	static void printTuple(int tuple_count,Tuple t) throws Exception {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println("Skyline tuple # "+tuple_count);
		System.out.println("-----------------------------------");
		t.print(_in);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
	}
	static void performNestedLoopsSky(AttrType[] in, short[] Ssizes, FldSpec[] projection,int[] pref_list,int pref_list_length,
																		String relationName, int n_pages) {
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
			//n_pages - 2 : since FileScan (nlScan) will use 2 buffer pages
			NestedLoopsSky nlSky = new NestedLoopsSky(in, in.length, Ssizes,
							nlScan, relationName, pref_list, pref_list_length, n_pages-2);
			System.out.println("**************************************************");
			System.out.println("\t NESTED LOOP SKYLINE (With dominated tuples stored in the buffer)");
			System.out.println("**************************************************");

			Tuple nestedLoopSkyline;
			int tuple_count = 1;
			while ((nestedLoopSkyline = nlSky.get_next()) != null) {
				printTuple(tuple_count,nestedLoopSkyline);
				tuple_count++;
			}
			nlSky.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		printDiskAccesses();
	}
	static void performBlockNestedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection,int[] pref_list,int pref_list_length,
			String relationName, int n_pages) {
		 FileScan am2 = null;
		    try {
		      am2 = new FileScan(relationName, in, Ssizes,
		              (short) in.length, (short) in.length,
		              projection, null);
		    } catch (Exception e) {
		    	e.printStackTrace();
		    }
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

		        Tuple t1 = null;
			    int tuple_count = 1;
				try {
					while ((t1 = sky2.get_next()) != null) {
						printTuple(tuple_count,t1);
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
		    printDiskAccesses();
	}
	static void performBtreeSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection,int[] pref_list,int pref_list_length,
			String relationName, int n_pages) throws Exception {
		if(n_pages<6)throw new Exception("Not enough pages to create index");
	     // create the index file
		 BTreeFile btf = null;
	     try {
	      btf = new BTreeFile("BTreeIndex", AttrType.attrReal, 4, 1); 
	     }
	     catch (Exception e) {
	       e.printStackTrace();
	       Runtime.getRuntime().exit(1);
	     }
	     
	     RID rid = new RID();
	     float key = 0;
	     Tuple temp = null;
	     Scan scan = new Scan(new Heapfile(relationName));
	     try {
	       temp = scan.getNext(rid);
	     }
	     catch (Exception e) {
	       e.printStackTrace();
	     }

	     Tuple tt = new Tuple();
	     try {
	       tt.setHdr((short)in.length, in, Ssizes);
	     }
	     catch (Exception e) {
	       e.printStackTrace();
	     }

	     int sizett = tt.size();
	     tt = new Tuple(sizett);
	     try {
	       tt.setHdr((short) in.length, in, Ssizes);
	     }
	     catch (Exception e) {
	       e.printStackTrace();
	     }
	     while ( temp != null) {
	       
	       try {
	    	   tt.tupleCopy(temp);
	     	  float tmp = (float)TupleUtils.getPrefAttrSum(tt, in, (short)in.length, pref_list, pref_list_length);
	     	  
	     	  key = -tmp;
	     	//  System.out.println(key);
	       }
	       catch (Exception e) {
	    	   e.printStackTrace();
	       }
	       
	       try {
	    	   btf.insert(new RealKey(key), rid); 
	       }
	       catch (Exception e) {
	 	e.printStackTrace();
	       }

	       try {
	 	temp = scan.getNext(rid);
	       }
	       catch (Exception e) {
	 	e.printStackTrace();
	       }
	     }
	     // close the file scan
	     scan.closescan();
	     PCounter.initialize();
	    Iterator sc = new BTreeSortedSky(in, (short)in.length, Ssizes, null, 
	    	    		relationName, pref_list, pref_list_length,"BTreeIndex", n_pages-1);
	    Tuple t1 = null;
	    int tuple_count = 1;
		try {
			while ((t1 = sc.get_next()) != null) {
				printTuple(tuple_count,t1);
				tuple_count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sc.close();
		}
		printDiskAccesses();
	}
	static void performSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection,int[] pref_list,int pref_list_length,
			String relationName, int n_pages) throws Exception {
		
		Iterator am1 = new FileScan(relationName, in, Ssizes, (short) in.length, in.length, projection, null);
		PCounter.initialize();
		Iterator sc = new SortFirstSky(in, (short) in.length, Ssizes, am1, relationName, pref_list, pref_list_length, n_pages-2);
		Tuple t1 = null;
	    int tuple_count = 1;
		try {
			while ((t1 = sc.get_next()) != null) {
				printTuple(tuple_count,t1);
				tuple_count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sc.close();
		}
		printDiskAccesses();
	}
	public void performBtreeSky(AttrType[] in, short[] Ssizes, FldSpec[] projection,int[] pref_list,int pref_list_length,
			String relationName, int n_pages) throws Exception {
		String[] indexFiles = new String[pref_list_length];
		for(int i=0;i<pref_list_length;i++) {
			 BTreeFile btf = null;
		     try {
		    	 indexFiles[i]  = "BTreeIndex"+i;
		      btf = new BTreeFile("BTreeIndex"+i, AttrType.attrReal, 4, 1); 
		     }
		     catch (Exception e) {
		       e.printStackTrace();
		       Runtime.getRuntime().exit(1);
		     }
		     
		     RID rid = new RID();
		     float key = 0;
		     Tuple temp = null;
		     Scan scan = new Scan(new Heapfile(relationName));
		     try {
		       temp = scan.getNext(rid);
		     }
		     catch (Exception e) {
		       e.printStackTrace();
		     }
	
		     Tuple tt = new Tuple();
		     try {
		       tt.setHdr((short)in.length, in, Ssizes);
		     }
		     catch (Exception e) {
		       e.printStackTrace();
		     }
	
		     int sizett = tt.size();
		     tt = new Tuple(sizett);
		     try {
		       tt.setHdr((short) in.length, in, Ssizes);
		     }
		     catch (Exception e) {
		       e.printStackTrace();
		     }
		     while ( temp != null) {
		       
		       try {
		    	   tt.tupleCopy(temp);
		     	  float tmp = tt.getFloFld(pref_list[i]);
		     	  
		     	  key = -tmp;
		     	//  System.out.println(key);
		       }
		       catch (Exception e) {
		    	   e.printStackTrace();
		       }
		       
		       try {
		    	   btf.insert(new RealKey(key), rid); 
		       }
		       catch (Exception e) {
		 	e.printStackTrace();
		       }
	
		       try {
		 	temp = scan.getNext(rid);
		       }
		       catch (Exception e) {
		 	e.printStackTrace();
		       }
		     }
		     // close the file scan
		     scan.closescan();
	     }
		PCounter.initialize();		
		BTreeSky btScan = new BTreeSky(in,(short)in.length,Ssizes,null,relationName,pref_list,pref_list_length,indexFiles,n_pages);
		Tuple t1 = null;
	    int tuple_count = 1;
		try {
			while ((t1 = btScan.get_next()) != null) {
				printTuple(tuple_count,t1);
				tuple_count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			btScan.close();
		}
		printDiskAccesses();
		
	}
	static void printDiskAccesses() {
		System.out.println("Read Count: "+ PCounter.rcounter);

		System.out.println("Write Count: "+PCounter.wcounter);
	}
}
