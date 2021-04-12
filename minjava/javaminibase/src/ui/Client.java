package ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.RealKey;
import btree.StringKey;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.Catalogglobal;
import global.GlobalConst;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import hash.ClusteredLinearHash;
import hash.UnclusteredLinearHash;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.BTreeSky;
import iterator.BTreeSortedSky;
import iterator.BlockNestedLoopSky;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.HashJoins;
import iterator.Iterator;
import iterator.NestedLoopsSky;
import iterator.RelSpec;
import iterator.SortFirstSky;
import iterator.TopK_HashJoin;
import iterator.TupleUtils;

public class Client {

	static final String relationName = "data";
	public static boolean flag = false;
//	static int n_pages;
//	static int[] pref_list;
//	static int pref_list_length;
//	static AttrType[] _in;
//	static FldSpec[] projection;
	static SystemDefs sys;
	
	static AttrType[] rel_attrs;
	static short[] rel_str_lens;
	
	static AttrType[] attr_attrs;
	static short[] attr_str_lens;
	
	static AttrType[] ind_attrs;
	static short[] ind_str_lens;
	
	static AttrType[] curr_in;
	static short[] curr_str_lens;
	static String[] curr_attr_names;
	
	static final String prefix_file_path="/afs/asu.edu/users/j/t/r/jtrada/";
	

	public static void main(String args[]) {
		
		rel_attrs = new AttrType[3];
		rel_attrs[0] = new AttrType(AttrType.attrString);
		rel_attrs[1] = new AttrType(AttrType.attrInteger);
		rel_attrs[2] = new AttrType(AttrType.attrInteger);
		
		rel_str_lens = new short[1];
		rel_str_lens[0] = GlobalConst.MAX_NAME+2;
		
		attr_attrs = new AttrType[4];
		attr_attrs[0] = new AttrType(AttrType.attrString);
		attr_attrs[1] = new AttrType(AttrType.attrString);
		attr_attrs[2] = new AttrType(AttrType.attrInteger);
		attr_attrs[3] = new AttrType(AttrType.attrInteger);
		
		attr_str_lens = new short[2];
		attr_str_lens[0] = 20;
		attr_str_lens[1] = 20;
		
		ind_attrs = new AttrType[7];
		ind_attrs[0] = new AttrType(AttrType.attrString);
		ind_attrs[1] = new AttrType(AttrType.attrInteger);
		ind_attrs[2] = new AttrType(AttrType.attrInteger);
		ind_attrs[3] = new AttrType(AttrType.attrInteger);
		ind_attrs[4] = new AttrType(AttrType.attrInteger);
		ind_attrs[5] = new AttrType(AttrType.attrInteger);
		ind_attrs[6] = new AttrType(AttrType.attrInteger);
		
		ind_str_lens = new short[1];
		ind_str_lens[0] = GlobalConst.MAX_NAME+2;
		
		
		Scanner in = new Scanner(System.in);
		while (true) {
			//printMenu();
			String str = in.nextLine();
			String queryParts[] = str.trim().split("\\s+");
			if (queryParts.length == 0)
				continue;
			switch (queryParts[0]) {
			case "open_database":
				if (queryParts.length < 1) {
					errorQueryMessage();
					continue;
				}
				String dbname = queryParts[1];
				openDB(dbname);
				continue;
			case "close_database":
				if(sys==null) {
					System.out.println("please Insert the name of db which needs to be closed.");
					String name = in.nextLine();
					closeDB(name);
					continue;
				}
				try {
					sys.closeDB();
				}catch(Exception e) {
					System.out.println("Error closing the DB");
				}
				System.out.println("DB closed successfully.");
				continue;
			case "create_table":
				if(queryParts.length==2) {
					try {
						createTable(queryParts[1]);
					} catch (Exception e) {
						System.out.println("Error in creating the table");
						e.printStackTrace();
					}
				}else if(queryParts.length==5){
					try {
						createTableWithIndex(queryParts[4],Integer.parseInt(queryParts[3]),queryParts[2].equalsIgnoreCase("btree")?IndexType.B_Index:IndexType.Hash);
					} catch (Exception e) {
						System.out.println("Error in creating the table with clustered index");
					}
				}else {
					errorQueryMessage();
				}
				continue;
			case "create_index":
				if(queryParts.length==4) {
					try {
						createIndex(queryParts[3], Integer.parseInt(queryParts[2]), queryParts[1].equalsIgnoreCase("btree")?IndexType.B_Index:IndexType.Hash);
					} catch (Exception e) {
						System.out.println("Error creating the index file");
					}
				}else {
					errorQueryMessage();
				}
				continue;
			case "insert_data":
				if(queryParts.length==3) {
					try {
						insertData(queryParts[1], queryParts[2]);
					} catch (Exception e) {
						System.out.println("Error deleting the data.");
					}
				}else {
					errorQueryMessage();
				}
				continue;
			case "delete_data":
				if(queryParts.length==3) {
					try {
						deleteData(queryParts[1], queryParts[2]);
					} catch (Exception e) {
						System.out.println("Error deleting the data.");
					}
				}else {
					errorQueryMessage();
				}
				continue;
			case "output_table":
				if(queryParts.length==2) {
					try {
						printTable(queryParts[1]);
					} catch (Exception e) {
						System.out.println("Error printing the table");
					}
				}else {
					errorQueryMessage();
				}
				continue;
			case "output_index":
				if(queryParts.length==3) {
					outputIndex(queryParts[1], Integer.parseInt(queryParts[2]));
				}else {
					errorQueryMessage();
				}
				continue;
			case "groupby":
				if(queryParts.length==7) {
					groupby(queryParts[1], queryParts[2], Integer.parseInt(queryParts[3]), queryParts[4], queryParts[5], Integer.parseInt(queryParts[6]), "");
				}else if(queryParts.length==9) {
					groupby(queryParts[1], queryParts[2], Integer.parseInt(queryParts[3]), queryParts[4], queryParts[5], Integer.parseInt(queryParts[6]), queryParts[8]);
				}else {
					errorQueryMessage();
				}
				continue;
			case "skyline":
				if(queryParts.length==7) {
					computeSkyline(queryParts[1], queryParts[2], queryParts[3], Integer.parseInt(queryParts[4]), queryParts[6]);
				}else if(queryParts.length==5){
					computeSkyline(queryParts[1], queryParts[2], queryParts[3], Integer.parseInt(queryParts[4]), "");					
				}else {
					errorQueryMessage();
				}
				continue;
			case "join":
				if(queryParts.length==8) {
					try {
						join(queryParts[1], queryParts[2], Integer.parseInt(queryParts[3]), queryParts[4], Integer.parseInt(queryParts[5]), queryParts[6], Integer.parseInt(queryParts[7]), "");
					} catch (Exception e) {
						System.out.println("Error joining the tables");
						e.printStackTrace();
					}
				}else if(queryParts.length==10) {
					
				}else{
					errorQueryMessage();
				}
				continue;
			case "TOPKJOIN":
				if(queryParts.length==10) {
					try {
						topkjoin(queryParts[1],Integer.parseInt(queryParts[2]),queryParts[3],Integer.parseInt(queryParts[4]),Integer.parseInt(queryParts[5]),queryParts[6],Integer.parseInt(queryParts[7]),Integer.parseInt(queryParts[8]),Integer.parseInt(queryParts[9]),"");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else if(queryParts.length == 12) {
					
				}
				continue;
			case "EXIT":
				if(sys!=null) {
					try {
						sys.closeDB();
					} catch (Exception e) {
						System.out.println("Error Closing the database");
					}
				}
				break;
			default:
				continue;
			}
			break;
		}
		in.close();
	}
	
	private static void topkjoin(String joinType, int k, String outerRelation, int outer_join_attr, int outer_merge_attr, String innerRelation, int inner_join_attr, int inner_merge_attr, int n_pages, String outputTableName) throws Exception{
		if(joinType.equalsIgnoreCase("hash")) {
			getRelationAttrInfo(outerRelation);
			AttrType[] outer_in = curr_in.clone();
			short[] outer_strLens = curr_str_lens.clone();
			getRelationAttrInfo(innerRelation);
			AttrType[] inner_in = curr_in.clone();
			short[] inner_strLens = curr_str_lens.clone();
			
			int len_in1 = outer_in.length;
			int len_in2 = inner_in.length;
			int n_out_flds = len_in1 + len_in2;
			FldSpec proj_list[] = new FldSpec[n_out_flds];
			AttrType[] output_attr = new AttrType[n_out_flds];
			
			int pref_list_length=2;
			int[] pref_list = new int[pref_list_length];
			int j=0;
			for(int i=0;i<len_in1;i++) {
				proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
				output_attr[i] = outer_in[i];
				if(i==outer_merge_attr-1) {
					pref_list[j++] = outer_merge_attr;
				}
			}
			for(int i=0;i<len_in2;i++) {
				proj_list[i+len_in1] = new FldSpec(new RelSpec(RelSpec.innerRel), i+1);
				output_attr[i+len_in1] = inner_in[i];
				if(i==inner_merge_attr-1) {
					pref_list[j++] = len_in1+i+1;
				}
			}
			
			short output_str_lens[] = new short[outer_strLens.length+inner_strLens.length];
			j=0;
			for(short value:outer_strLens) {
				output_str_lens[j++] = value;
			}
			for(short value:inner_strLens) {
				output_str_lens[j++] = value;			
			}
			
			
			Iterator it = new TopK_HashJoin(outer_in,outer_in.length,outer_strLens,outer_join_attr,outer_merge_attr,inner_in,inner_in.length,inner_strLens,inner_join_attr,inner_merge_attr,outerRelation,innerRelation,n_out_flds,pref_list_length,pref_list,proj_list,output_attr,output_str_lens,k,n_pages);
			Tuple t = new Tuple();
			while((t=it.get_next())!=null) {
				t.setHdr((short)n_out_flds, output_attr, output_str_lens);
				t.print(output_attr);
			}
		}
	}
	
	private static void join(String joinType, String outerRelation, int outer_attr, String innerRelation, int inner_attr, String operator, int n_pages, String outputRelation) throws Exception{
		getRelationAttrInfo(outerRelation);
		AttrType[] outer_in = curr_in.clone();
		short[] outer_strLens = curr_str_lens.clone();
		getRelationAttrInfo(innerRelation);
		AttrType[] inner_in = curr_in.clone();
		short[] inner_strLens = curr_str_lens.clone();
		CondExpr[] outFilter = new CondExpr[2];

		outFilter[0] = new CondExpr();
		outFilter[0].next  = null;
		if(operator.equalsIgnoreCase("=")) {
			outFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);	
		}else if(operator.equalsIgnoreCase("<")) {
			outFilter[0].op    = new AttrOperator(AttrOperator.aopLT);
		}else if(operator.equalsIgnoreCase("<=")) {
			outFilter[0].op    = new AttrOperator(AttrOperator.aopLE);
		}else if(operator.equalsIgnoreCase(">")) {
			outFilter[0].op    = new AttrOperator(AttrOperator.aopGT);
		}else if(operator.equalsIgnoreCase(">=")) {
			outFilter[0].op    = new AttrOperator(AttrOperator.aopGE);
		}
		outFilter[1] = null;
		
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),outer_attr);
		
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),inner_attr);
		
		CondExpr[] rightFilter = null;
		
		int len_in1 = outer_in.length;
		int len_in2 = inner_in.length;
		int n_out_flds = len_in1 + len_in2;
		FldSpec proj_list[] = new FldSpec[n_out_flds];
		AttrType[] output_attr = new AttrType[n_out_flds];
		for(int i=0;i<len_in1;i++) {
			proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
			output_attr[i] = outer_in[i];
		}
		for(int i=0;i<len_in2;i++) {
			proj_list[i+len_in1] = new FldSpec(new RelSpec(RelSpec.innerRel), i+1);
			output_attr[i+len_in1] = inner_in[i];
		}
		Iterator scan = null;
		switch(joinType) {
		case "HJ":
			scan = new HashJoins(outer_in, len_in1, outer_strLens, inner_in, len_in2, inner_strLens, n_pages, outerRelation, innerRelation, outFilter, rightFilter, proj_list, n_out_flds);
			Tuple t = null;
			while((t=scan.get_next())!=null) {
				t.print(output_attr);
			}
			break;
		}
		scan.close();
		
	}
	
	private static void computeSkyline(String type, String pref_list, String relationName, int n_pages, String outputRelation) {
		
	}
	private static void closeDB(String name) {
		String dbpath = prefix_file_path + System.getProperty("user.name") + ".minibase."+name;
		File file = new File(dbpath);
		if (file.exists()) {
			sys = new SystemDefs(dbpath, 0, GlobalConst.NUMBUF, "Clock");
			try {
				sys.closeDB();
			}catch(Exception e) {
				System.out.println("Error closing the database");
			}
			System.out.println("Closed the database");
		}else {
			System.out.println("DB does not exist.");
		}
	}
	
	private static void createTable(String fileName) throws Exception{
		if(sys==null) {
			System.out.println("Please open a database first.");
			return;
		}
		File file = new File(prefix_file_path+fileName+".txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		int numberOfCols = Integer.parseInt(br.readLine().trim());
		String attr_names[] = new String[numberOfCols];
		AttrType[] in = new AttrType[numberOfCols];
		int strnum=0;
		int i=0;
		while(i<numberOfCols) {
			String str[] = br.readLine().split("\\s+");
			attr_names[i] = str[0];
			switch(str[1]) {
			case "INT":
				in[i] = new AttrType(AttrType.attrInteger);
				break;
			case "STR":
				in[i] = new AttrType(AttrType.attrString);
				strnum++;
				break;
			default:
				in[i] = new AttrType(AttrType.attrReal);
				break;
			}
			i++;
		}
		short[] strsizes = new short[strnum];
		Arrays.fill(strsizes, (short)GlobalConst.MAX_STR_LEN);
		addRelationAttrInfo(fileName, attr_names, in, strsizes);
		
		Heapfile hf = new Heapfile(fileName);
		
		String str = null;
		Tuple t = new Tuple();
		t.setHdr((short)numberOfCols, in, strsizes);
		t = new Tuple(t.size());
		t.setHdr((short)numberOfCols, in, strsizes);
		while ((str = br.readLine()) != null) {
			String attrs[] = str.split("\\s+");

			int k = 1;

			for (String attr : attrs) {
				attr = attr.trim();
				if (attr.equals(""))
					continue;
				switch (in[k - 1].attrType) {
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
			hf.insertRecord(t.getTupleByteArray());
		}
		br.close();
		sys.flushBuffer();
	}
	private static void addRelationAttrInfo(String relationName, String[] attr_names, AttrType[] in, short[] strlens) throws Exception{
		Heapfile hf = new Heapfile(Catalogglobal.RELCATNAME);
		Tuple t = new Tuple();
		t.setHdr((short)rel_attrs.length, rel_attrs, rel_str_lens);
		t = new Tuple(t.size());
		t.setHdr((short)rel_attrs.length, rel_attrs, rel_str_lens);
		t.setStrFld(1, relationName);
		t.setIntFld(2, in.length);
		t.setIntFld(3, strlens.length);
		hf.insertRecord(t.getTupleByteArray());
		hf = new Heapfile(Catalogglobal.ATTRCATNAME);
		t = new Tuple();
		t.setHdr((short)attr_attrs.length, attr_attrs, attr_str_lens);
		int k=0;
		for(int i=0;i<in.length;i++) {
			t = new Tuple(t.size());
			t.setHdr((short)attr_attrs.length, attr_attrs, attr_str_lens);
			t.setStrFld(1, relationName);
			t.setStrFld(2, attr_names[i]);
			t.setIntFld(3, in[i].attrType);
			t.setIntFld(4, in[i].attrType==AttrType.attrString?strlens[k++]:4);
			hf.insertRecord(t.getTupleByteArray());
		}
		
	}
	
	private static void getRelationAttrInfo(String relationName) throws Exception{
		Heapfile hf = new Heapfile(Catalogglobal.RELCATNAME);
		Scan scan = new Scan(hf);
		Tuple t = null;
		while((t = scan.getNext(new RID()))!=null) {
			t.setHdr((short)rel_attrs.length, rel_attrs, rel_str_lens);
			if(t.getStrFld(1).equalsIgnoreCase(relationName)) {
				scan.closescan();
				int num_col = t.getIntFld(2);
				int numstr = t.getIntFld(3);
				curr_in = new AttrType[num_col];
				curr_attr_names = new String[num_col];
				curr_str_lens = new short[numstr];
				scan = new Scan(new Heapfile(Catalogglobal.ATTRCATNAME));
				int i=0;
				int j=0;
				while((t=scan.getNext(new RID()))!=null) {
					t.setHdr((short)attr_attrs.length, attr_attrs, attr_str_lens);
					if(t.getStrFld(1).equalsIgnoreCase(relationName)) {
						curr_attr_names[i] = t.getStrFld(2);
						curr_in[i++]=new AttrType(t.getIntFld(3));
						if(curr_in[i-1].attrType==AttrType.attrString) {
							curr_str_lens[j++] = (short)t.getIntFld(4);
						}
					}
				}
				scan.closescan();
			}

		}		
	}
	
	private static void addIndexInfo(String relationName, String indexName, int attr_num, int index_type, boolean clustered, int hash1, int numBuckets, int splitpointer) throws Exception{
		Heapfile hf = new Heapfile(Catalogglobal.INDEXCATNAME);
		Tuple t = new Tuple();
		t.setHdr((short)ind_attrs.length, ind_attrs, ind_str_lens);
		t = new Tuple(t.size());
		t.setHdr((short)ind_attrs.length, ind_attrs, ind_str_lens);
		t.setStrFld(1, relationName);
		t.setIntFld(2, index_type);
		t.setIntFld(3, attr_num);
		t.setIntFld(4, clustered?1:0);
		t.setIntFld(5, hash1);
		t.setIntFld(6, numBuckets);
		t.setIntFld(7, splitpointer);
		hf.insertRecord(t.getTupleByteArray());
	}
	
	private static void getIndexInfo(String indexName) {
		
	}
	
	
	
	private static void createTableWithIndex(String fileName, int attr_num, int indexType) throws Exception{
		String indexFileName = "clst_";
		switch(indexType) {
		case IndexType.B_Index:
			indexFileName+="bt_";
			break;
		case IndexType.Hash:
			indexFileName+="hs_";
			break;
		}
		indexFileName+=fileName+"_"+attr_num;
		if(indexType==IndexType.Hash) {
			if(sys==null) {
				System.out.println("Please open a database first.");
				return;
			}
			String filepath = prefix_file_path+fileName+".txt"; 
			File file = new File(filepath);
			BufferedReader br = new BufferedReader(new FileReader(file));
			int numberOfCols = Integer.parseInt(br.readLine().trim());
			String attr_names[] = new String[numberOfCols];
			AttrType[] in = new AttrType[numberOfCols];
			int strnum=0;
			int i=0;
			while(i<numberOfCols) {
				String str[] = br.readLine().split("\\s+");
				attr_names[i] = str[0];
				switch(str[1]) {
				case "INT":
					in[i] = new AttrType(AttrType.attrInteger);
					break;
				case "STR":
					in[i] = new AttrType(AttrType.attrString);
					strnum++;
					break;
				default:
					in[i] = new AttrType(AttrType.attrReal);
					break;
				}
				i++;
			}
			short[] strsizes = new short[strnum];
			Arrays.fill(strsizes, (short)GlobalConst.MAX_STR_LEN);
			addRelationAttrInfo(fileName, attr_names, in, strsizes);
			ClusteredLinearHash clh = new ClusteredLinearHash(-1, filepath, attr_num, fileName, indexFileName);
			addIndexInfo(fileName,indexFileName,attr_num,IndexType.Hash,true,clh.hash1,clh.numBuckets,clh.splitPointer);
			br.close();
		}else {
			
		}
	}
	
	private static void createIndex(String tableName, int attr_num, int indexType) throws Exception{
		String indexFileName = "uclst_";
		switch(indexType) {
		case IndexType.B_Index:
			indexFileName+="bt_";
			break;
		case IndexType.Hash:
			indexFileName+="hs_";
			break;
		}
		indexFileName+=tableName+"_"+attr_num;
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		short[] strSizes = curr_str_lens.clone();
		
		if(indexType==IndexType.B_Index) {
			BTreeFile btf = null;
			try {
				btf = new BTreeFile(indexFileName, in[attr_num-1].attrType, in[attr_num-1].attrType==AttrType.attrString?strSizes[0]:4, 1);
			} catch (Exception e) {
				e.printStackTrace();
			}
			//Begin sorting process	    
			RID rid = new RID();
			Tuple temp = null;
			Scan scan = new Scan(new Heapfile(tableName));
			try {
				temp = scan.getNext(rid);
			} catch (Exception e) {
				e.printStackTrace();
			}

			Tuple tt = new Tuple();
			try {
				tt.setHdr((short) in.length, in, strSizes);
			} catch (Exception e) {
				e.printStackTrace();
			}

			int sizett = tt.size();
			tt = new Tuple(sizett);
			try {
				tt.setHdr((short) in.length, in, strSizes);
			} catch (Exception e) {
				e.printStackTrace();
			}
			while (temp != null) {

				try {
					tt.tupleCopy(temp);
					switch(in[attr_num-1].attrType) {
					case AttrType.attrInteger:
						btf.insert(new IntegerKey(tt.getIntFld(attr_num)), rid);
						break;
					case AttrType.attrReal:
						btf.insert(new RealKey(tt.getFloFld(attr_num)), rid);
						break;
					case AttrType.attrString:
						btf.insert(new StringKey(tt.getStrFld(attr_num)), rid);
						break;
					}
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
			btf.close();
			// close the file scan
			scan.closescan();
			addIndexInfo(tableName,indexFileName,attr_num,IndexType.B_Index,false,0,0,0);
		}else {
			UnclusteredLinearHash ulh = new UnclusteredLinearHash(-1, tableName, attr_num, strSizes, in, indexFileName);
			addIndexInfo(tableName,indexFileName,attr_num,IndexType.Hash,false,ulh.hash1,ulh.numBuckets,ulh.splitPointer);			
		}
		System.out.println("Index Created Successfully.");
		sys.flushBuffer();
	}
	
	private static void insertData(String tableName, String fileName) throws Exception{
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		short[] strLens = curr_str_lens.clone();
		String[] attr_names = curr_attr_names.clone();
		
	}
	private static void deleteData(String tableName, String fileName) throws Exception{
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		short[] strLens = curr_str_lens.clone();
		String[] attr_names = curr_attr_names.clone();
		
	}
	private static void printTable(String tableName) throws Exception{
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		short[] strLens = curr_str_lens.clone();
		String[] attr_names = curr_attr_names.clone();
		Tuple t = null;
		Scan scan = new Scan(new Heapfile(tableName));
		System.out.println("===="+tableName+"====");
		System.out.println(attr_names.toString());
		while((t = scan.getNext(new RID()))!=null) {
			t.setHdr((short)attr_names.length, in, strLens);
			t.print(in);
		}
	}
	private static void outputIndex(String tableName, int attr_num) {
		
	}
	private static void groupby(String type, String agg_type, int agg_attr_num, String attr_list, String tableName, int n_pages, String outputTable) {
		
	}
	private static void errorQueryMessage() {
		System.out.println("Not a valid query, please write again!");
	}
	
	private static void printMenu() {
		System.out.println("\n======Sample queries=========\n");
		System.out.println("1> open_database DBNAME");
		System.out.println("2> close_database");
		System.out.println("3> create_table [clustered btree/hash att_no] fileName");
		System.out.println("4> create_index Btree/hash att_no tablename");
		System.out.println("5> insert_data tablename filename");
		System.out.println("6> delete_data tablename filename");
		System.out.println("7> output_table tablename");
		System.out.println("8> output_index tablename att_no");
		System.out
				.println("9> skyline NLS/BNLS/SFS/BS/BSS {att1,att2,...,atth} tablename npages [mater outputtablename]");
		System.out.println(
				"10> groupby sort/hash max/min/agg/sky g_attr_no {att1,att2,...,atth} tablename npages [mater outputtablename]");
		System.out.println(
				"11> join NLJ/SMJ/INLJ/HJ OTABLENAME O_ATT_NO ITABLENAME I_ATT_NO OP(\"=\"/\"<=\"/\"<\"/\">=\"/\">\") npages [mater outputtablename]");
		System.out.println(
				"12> TOPKJOIN hash/nra k OTABLENAME O_J_att_no o_M_att_no Itablename I_J_att_no I_M_att_no npages [mater outputtablename]");
		System.out.println("13> Type \"EXIT\" to terminate the session\n");
	}

	static void openDB(String dbName) {
		String dbpath = prefix_file_path + System.getProperty("user.name") + ".minibase."+dbName;
		File file = new File(dbpath);
		if (file.exists()) {
			sys = new SystemDefs(dbpath, 0, GlobalConst.NUMBUF, "Clock");
			System.out.println("Opened an existing database");
		}else {
			sys = new SystemDefs(dbpath, 100000, GlobalConst.NUMBUF, "Clock");
			System.out.println("Created a new database");
		}
	}


	// Print Skyline tuples
	static void printTuple(int tuple_count, Tuple t) throws Exception {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println("Skyline tuple # " + tuple_count);
		System.out.println("-----------------------------------");
//		t.print(_in);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
	}

	// Start NestedLoopsSky
	static void performNestedLoopsSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages) throws IndexException, IOException {
		// For scanning outer loop
		sys.flushBuffer();
		PCounter.initialize();
		FileScan nlScan = null;
		try {
			nlScan = new FileScan(relationName, in, Ssizes, (short) in.length, (short) in.length, projection, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		NestedLoopsSky nlSky = null;
		try {
			nlSky = new NestedLoopsSky(in, in.length, Ssizes, nlScan, relationName, pref_list, pref_list_length,
					n_pages);
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
		} finally {
			if (nlScan != null)
				nlScan.close();
			if (nlSky != null)
				nlSky.close();
		}
		// Print the read / write count on disk
		printDiskAccesses();
	}

	// Start BlockNestedLoopSky
	static void performBlockNestedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages) {
		FileScan am2 = null;
		BlockNestedLoopSky sky2 = null;
		sys.flushBuffer();

		PCounter.initialize();
		try {
			// flag = true;
			sky2 = new BlockNestedLoopSky(in, in.length, Ssizes, am2, relationName, pref_list, pref_list_length,
					n_pages);
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
	static void performBtreeSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages) throws Exception {

		if (sys.getUnpinCount() < 6)
			throw new Exception("Not enough pages to create index");
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
				// System.out.println(key);
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
		Iterator sc = new BTreeSortedSky(in, (short) in.length, Ssizes, null, relationName, pref_list, pref_list_length,
				"BTreeIndex", n_pages);
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
	static void performSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages) throws Exception {
		sys.flushBuffer();

		PCounter.initialize();

		Iterator am1 = new FileScan(relationName, in, Ssizes, (short) in.length, in.length, projection, null);
		Iterator sc = null;
		try {
			sc = new SortFirstSky(in, (short) in.length, Ssizes, am1, relationName, pref_list, pref_list_length,
					n_pages - 2);
		} catch (Exception e) {
			if (am1 != null)
				am1.close();
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
	static void performBtreeSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages) throws Exception {
//////checking page limits for construction of index	  
		if (sys.getUnpinCount() < 6)
			throw new Exception("Not enough pages to create index");
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
					// System.out.println(key);
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

		BTreeSky btScan = new BTreeSky(in, (short) in.length, Ssizes, null, relationName, pref_list, pref_list_length,
				indexFiles, n_pages);

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
