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

			System.out.println("performNestedLoopSky START::");
			setupDB();
			printDiskAccesses();
			try {
				performNestedLoopSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			printDiskAccesses();
			System.out.println("performNestedLoopSky END::");
			
			System.out.println("performBlockNestedSky START::");
			setupDB();
			printDiskAccesses();
			try {
				performBlockNestedSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			printDiskAccesses();
			System.out.println("performBlockNestedSky END::");
			
			System.out.println("performSortedSky START::");
			setupDB();
			printDiskAccesses();
			try {
				performSortedSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			printDiskAccesses();
			System.out.println("performSortedSky END::");
			
			System.out.println("performBtreeSortedSky START::");
			setupDB();
			printDiskAccesses();
			try {
				performBtreeSortedSky(_in, new short[1], projection, pref_list, pref_list_length, relationName, n_pages);
			}catch(Exception e) {
				e.printStackTrace();
			}
			printDiskAccesses();
			System.out.println("performBtreeSortedSky END::");
			
			
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

	    new SystemDefs( dbpath, 100000, 500, "Clock" );
	    
	    File file = new File("/afs/asu.edu/users/j/t/r/jtrada/data.txt");
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
				t.setFloFld(k++, Float.parseFloat(attr));
			}
			try {
				f.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}
		}
		br.close();
	}

	static void performNestedLoopSky(AttrType[] in, short[] Ssizes, FldSpec[] projection,int[] pref_list,int pref_list_length,
																		String relationName, int n_pages) {
		FileScan nlsf = null;
		try {
			nlsf = new FileScan(relationName, in, Ssizes,
							(short) in.length, (short) in.length,
							projection, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		NestedLoopsSky nlSky = null;
		try {
			nlSky = new NestedLoopsSky(in, in.length, Ssizes,
							nlsf, relationName, pref_list, pref_list_length, n_pages);
			System.out.println("**************************************************");
			System.out.println("\t\t NESTED LOOP SKYLINE ");
			System.out.println("**************************************************");

			Tuple nestedLoopSkyline;
			int tuple_count = 0;
			while ((nestedLoopSkyline = nlSky.get_next()) != null) {
				System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
				System.out.println("Skyline tuple # "+tuple_count);
				System.out.println("-----------------------------------");
				nestedLoopSkyline.print(in);
				System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
				tuple_count++;
			}
			nlSky.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
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
		    try {

		        sky2 = new BlockNestedLoopSky(in, in.length, Ssizes,
		                am2, relationName, pref_list, pref_list_length, n_pages);
		        System.out.println("**************************************************");
		        System.out.println("**************************************************");
		        System.out.println("\t\tBLOCK NESTED SKYLINE ");
		        System.out.println("**************************************************");
		        System.out.println("**************************************************\n");

		        Vector<Tuple> skyline;
		        int batch = 1;
		        while ((skyline = sky2.get_skyline()) != null) {
		          System.out.println("\n************* SKYLINE BATCH " + batch + " ***************\n");
		          for (int i = 0; i < skyline.size(); i++) {
//		            System.out.print((i + 1) + ". ");
//		            skyline.get(i).print(in);
		        	  System.out.println(TupleUtils.getPrefAttrSum(skyline.get(i), in, (short) in.length, pref_list, pref_list_length));
						
		          }
		          batch++;
		        }
		      } catch (Exception e) {
		        e.printStackTrace();
		      }
	}
	static void performBtreeSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection,int[] pref_list,int pref_list_length,
			String relationName, int n_pages) throws Exception {
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
	    Iterator sc = new BTreeSortedSky(in, (short)in.length, Ssizes, null, 
	    	    		relationName, pref_list, pref_list_length,"BTreeIndex", n_pages);
	    
	     Tuple t1 = null;
		try {
			while ((t1 = sc.get_next()) != null) {
//				System.out.println(TupleUtils.getPrefAttrSum(t1, in, (short) in.length, pref_list, pref_list_length));
				t1.print(in);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sc.close();
		}
	}
	static void performSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection,int[] pref_list,int pref_list_length,
			String relationName, int n_pages) throws Exception {
		
		Iterator am1 = new FileScan(relationName, in, Ssizes, (short) in.length, in.length, projection, null);
		Iterator sc = new SortFirstSky(in, (short) in.length, Ssizes, am1, relationName, pref_list, pref_list_length, n_pages);

		Tuple t1 = null;
		try {
			while ((t1 = sc.get_next()) != null) {
				System.out.println(TupleUtils.getPrefAttrSum(t1, in, (short) in.length, pref_list, pref_list_length));
				t1.print(in);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			am1.close();
			sc.close();
		}
	}
	
	static void printDiskAccesses() {
		System.out.println("Read Count: "+ PCounter.rcounter);

		System.out.println("Write Count: "+PCounter.wcounter);
	}
}
