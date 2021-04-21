package ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import btree.BT;
import btree.BTreeFile;
import btree.ClusteredBtreeIndex;
import btree.IntegerKey;
import btree.KeyClass;
import btree.RealKey;
import btree.StringKey;
import diskmgr.PCounter;
import diskmgr.Page;
import global.AggType;
import global.AttrOperator;
import global.AttrType;
import global.Catalogglobal;
import global.GlobalConst;
import global.IndexInfo;
import global.IndexType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import hash.ClusteredHashIndexScan;
import hash.ClusteredLinearHash;
import hash.UnclusteredHashIndexScan;
import hash.UnclusteredLinearHash;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import iterator.*;

public class Client {

	public static boolean flag = false;
	static SystemDefs sys;

	static AttrType[] rel_attrs;
	static short[] rel_str_lens;
	static Heapfile relationHF;

	static AttrType[] attr_attrs;
	static short[] attr_str_lens;
	static Heapfile attrHF;

	static AttrType[] ind_attrs;
	static short[] ind_str_lens;
	static Heapfile indHF;

	static AttrType[] curr_in;
	static short[] curr_str_lens;
	static String[] curr_attr_names;

	static final String prefix_file_path = "/afs/asu.edu/users/j/t/r/jtrada/";

	public static void main(String args[]) {

		rel_attrs = new AttrType[3];
		rel_attrs[0] = new AttrType(AttrType.attrString);
		rel_attrs[1] = new AttrType(AttrType.attrInteger);
		rel_attrs[2] = new AttrType(AttrType.attrInteger);

		rel_str_lens = new short[1];
		rel_str_lens[0] = GlobalConst.MAX_STR_LEN + 2;

		attr_attrs = new AttrType[4];
		attr_attrs[0] = new AttrType(AttrType.attrString);
		attr_attrs[1] = new AttrType(AttrType.attrString);
		attr_attrs[2] = new AttrType(AttrType.attrInteger);
		attr_attrs[3] = new AttrType(AttrType.attrInteger);

		attr_str_lens = new short[2];
		attr_str_lens[0] = GlobalConst.MAX_STR_LEN + 2;
		attr_str_lens[1] = GlobalConst.MAX_STR_LEN + 2;

		ind_attrs = new AttrType[7];
		ind_attrs[0] = new AttrType(AttrType.attrString);
		ind_attrs[1] = new AttrType(AttrType.attrInteger);
		ind_attrs[2] = new AttrType(AttrType.attrInteger);
		ind_attrs[3] = new AttrType(AttrType.attrInteger);
		ind_attrs[4] = new AttrType(AttrType.attrInteger);
		ind_attrs[5] = new AttrType(AttrType.attrInteger);
		ind_attrs[6] = new AttrType(AttrType.attrInteger);

		ind_str_lens = new short[1];
		ind_str_lens[0] = GlobalConst.MAX_STR_LEN + 2;

		Scanner in = new Scanner(System.in);
		while (true) {
			printMenu();
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
				if (sys == null) {
					System.out.println("No open database to be closed");
					continue;
				}
				try {
					sys.closeDB();
				} catch (Exception e) {
					System.out.println("Error closing the DB");
				}
				System.out.println("DB closed successfully.");
				continue;
			case "create_table":
				PCounter.initialize();
				if (queryParts.length == 2) {
					try {
						createTable(queryParts[1]);
					} catch (Exception e) {
						System.out.println("Error in creating the table");
						e.printStackTrace();
					}
				} else if (queryParts.length == 5) {
					try {
						createTableWithIndex(queryParts[4], Integer.parseInt(queryParts[3]),
								queryParts[2].equalsIgnoreCase("btree") ? IndexType.B_Index : IndexType.Hash);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Error in creating the table with clustered index");
					}
				} else {
					errorQueryMessage();
				}
				if(sys!=null) {
					sys.flushBuffer();
				}
				printDiskAccesses();
				continue;
			case "create_index":
				PCounter.initialize();
				if (queryParts.length == 4) {
					try {
						createIndex(queryParts[3], Integer.parseInt(queryParts[2]),
								queryParts[1].equalsIgnoreCase("btree") ? IndexType.B_Index : IndexType.Hash);
					} catch (Exception e) {
						System.out.println("Error creating the index file");
						e.printStackTrace();
					}
				} else {
					errorQueryMessage();
				}
				if(sys!=null) {
					sys.flushBuffer();
				}
				printDiskAccesses();
				continue;
			case "insert_data":
				PCounter.initialize();
				if (queryParts.length == 3) {
					try {
						insertData(queryParts[1], queryParts[2]);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Error inserting the data.");
					}
				} else {
					errorQueryMessage();
				}
				if(sys!=null) {
					sys.flushBuffer();
				}
				printDiskAccesses();
				continue;
			case "delete_data":
				PCounter.initialize();
				if (queryParts.length == 3) {
					try {
						deleteData(queryParts[1], queryParts[2]);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Error deleting the data.");
					}
				} else {
					errorQueryMessage();
				}
				if(sys!=null) {
					sys.flushBuffer();
				}
				printDiskAccesses();
				continue;
			case "output_table":
				PCounter.initialize();
				if (queryParts.length == 2) {
					try {
						printTable(queryParts[1]);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Error printing the table");
					}
				} else {
					errorQueryMessage();
				}
				
				if(sys!=null) {
					sys.flushBuffer();
				}
				printDiskAccesses();
				continue;
			case "output_index":
				PCounter.initialize();
				if (queryParts.length == 3) {
					try {
						outputIndex(queryParts[1], Integer.parseInt(queryParts[2]));
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					errorQueryMessage();
				}
				if(sys!=null) {
					sys.flushBuffer();
				}
				printDiskAccesses();
				continue;
			case "groupby":
				if (queryParts.length == 7) {
					try {
						groupby(queryParts[1], queryParts[2], Integer.parseInt(queryParts[3]), queryParts[4],
								queryParts[5], Integer.parseInt(queryParts[6]), "");
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else if (queryParts.length == 9) {
					try {
						groupby(queryParts[1], queryParts[2], Integer.parseInt(queryParts[3]), queryParts[4],
								queryParts[5], Integer.parseInt(queryParts[6]), queryParts[8]);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					errorQueryMessage();
				}
				continue;
			case "skyline":
				if (queryParts.length == 7) {
					computeSkyline(queryParts[1], queryParts[2], queryParts[3], Integer.parseInt(queryParts[4]),
							queryParts[6]);
				} else if (queryParts.length == 5) {
					computeSkyline(queryParts[1], queryParts[2], queryParts[3], Integer.parseInt(queryParts[4]), "");
				} else {
					errorQueryMessage();
				}
				continue;
			case "join":
				if (queryParts.length == 8) {
					try {
						join(queryParts[1], queryParts[2], Integer.parseInt(queryParts[3]), queryParts[4],
								Integer.parseInt(queryParts[5]), queryParts[6], Integer.parseInt(queryParts[7]), "");
					} catch (Exception e) {
						System.out.println("Error joining the tables");
						e.printStackTrace();
					}
				} else if (queryParts.length == 10) {
					try {
						join(queryParts[1], queryParts[2], Integer.parseInt(queryParts[3]), queryParts[4],
								Integer.parseInt(queryParts[5]), queryParts[6], Integer.parseInt(queryParts[7]),
								queryParts[9]);
					} catch (Exception e) {
						System.out.println("Error joining the tables");
						e.printStackTrace();
					}
				} else {
					errorQueryMessage();
				}
				continue;
			case "TOPKJOIN":
				if (queryParts.length == 10) {
					try {
						topkjoin(queryParts[1], Integer.parseInt(queryParts[2]), queryParts[3],
								Integer.parseInt(queryParts[4]), Integer.parseInt(queryParts[5]), queryParts[6],
								Integer.parseInt(queryParts[7]), Integer.parseInt(queryParts[8]),
								Integer.parseInt(queryParts[9]), "");
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else if (queryParts.length == 12) {
					try {
						topkjoin(queryParts[1], Integer.parseInt(queryParts[2]), queryParts[3],
								Integer.parseInt(queryParts[4]), Integer.parseInt(queryParts[5]), queryParts[6],
								Integer.parseInt(queryParts[7]), Integer.parseInt(queryParts[8]),
								Integer.parseInt(queryParts[9]), queryParts[11]);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				continue;
			case "create":
				try {
					updateData(queryParts[1]);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				continue;
			case "EXIT":
				if (sys != null) {
					try {
						sys.closeDB();
					} catch (Exception e) {
						System.out.println("Error Closing the database");
					}
				}
				break;
			default:
				System.out.println("ClientDriver(): Invalid Query");
				continue;
			}
			break;
		}
		in.close();
	}

	private static void topkjoin(String joinType, int k, String outerRelation, int outer_join_attr,
			int outer_merge_attr, String innerRelation, int inner_join_attr, int inner_merge_attr, int n_pages,
			String outputTableName) throws Exception {
		if (joinType.equalsIgnoreCase("hash")) {
			getRelationAttrInfo(outerRelation);
			AttrType[] outer_in = curr_in.clone();
			short[] outer_strLens = curr_str_lens.clone();
			String[] outer_names = curr_attr_names.clone();
			getRelationAttrInfo(innerRelation);
			AttrType[] inner_in = curr_in.clone();
			short[] inner_strLens = curr_str_lens.clone();
			String[] inner_names = curr_attr_names.clone();
			String output_names[] = new String[outer_in.length+inner_in.length+1];
			int p=0;
			for(String tmp:outer_names) {
				output_names[p++] = tmp;
			}
			for(String tmp:inner_names) {
				output_names[p++] = tmp;
			}
			output_names[p] = "AVG";
			int len_in1 = outer_in.length;
			int len_in2 = inner_in.length;
			int n_out_flds = len_in1 + len_in2;
			FldSpec proj_list[] = new FldSpec[n_out_flds];
			AttrType[] output_attr = new AttrType[n_out_flds];

			int pref_list_length = 2;
			int[] pref_list = new int[pref_list_length];
			int j = 0;
			for (int i = 0; i < len_in1; i++) {
				proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
				output_attr[i] = outer_in[i];
				if (i == outer_merge_attr - 1) {
					pref_list[j++] = outer_merge_attr;
				}
			}
			for (int i = 0; i < len_in2; i++) {
				proj_list[i + len_in1] = new FldSpec(new RelSpec(RelSpec.innerRel), i + 1);
				output_attr[i + len_in1] = inner_in[i];
				if (i == inner_merge_attr - 1) {
					pref_list[j++] = len_in1 + i + 1;
				}
			}

			short output_str_lens[] = new short[outer_strLens.length + inner_strLens.length];
			j = 0;
			for (short value : outer_strLens) {
				output_str_lens[j++] = value;
			}
			for (short value : inner_strLens) {
				output_str_lens[j++] = value;
			}
			try {
				SystemDefs.JavabaseBM.flushAllPages();
			} catch (Exception e) {

			}
			PageId[] pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
			get_buffer_pages(pageIds.length, pageIds);
			PCounter.initialize();
			Iterator it = null;
			try {
				it = new TopK_HashJoin(outer_in, outer_in.length, outer_strLens, outer_join_attr, outer_merge_attr,
						inner_in, inner_in.length, inner_strLens, inner_join_attr, inner_merge_attr, outerRelation,
						innerRelation, n_out_flds, pref_list_length, pref_list, proj_list, output_attr, output_str_lens,
						k, n_pages);
				Tuple t = new Tuple();
				AttrType[] topk_out_attr = new AttrType[n_out_flds + 1];
				for (int i = 0; i < n_out_flds; i++) {
					topk_out_attr[i] = output_attr[i];
				}
				topk_out_attr[n_out_flds] = new AttrType(AttrType.attrReal);
				Tuple t1 = new Tuple();
				t1.setHdr((short) (n_out_flds + 1), topk_out_attr, output_str_lens);
				t1 = new Tuple(t1.size());
				Heapfile hf = null;
				if(!outputTableName.isEmpty()) {
					hf = new Heapfile(outputTableName);
					addRelationAttrInfo(outputTableName, output_names, topk_out_attr, output_str_lens);
				}
				while ((t = it.get_next()) != null) {
					t.setHdr((short) n_out_flds, output_attr, output_str_lens);
					t1.setHdr((short) (n_out_flds + 1), topk_out_attr, output_str_lens);
					for (int i = 0; i < n_out_flds; i++) {
						switch (output_attr[i].attrType) {
						case AttrType.attrInteger:
							t1.setIntFld(i + 1, t.getIntFld(i + 1));
							break;
						case AttrType.attrReal:
							t1.setFloFld(i + 1, t.getFloFld(i + 1));
							break;
						case AttrType.attrString:
							t1.setStrFld(i + 1, t.getStrFld(i + 1));
							break;
						}
					}
					float sum = 0;
					switch (output_attr[outer_merge_attr].attrType) {
					case AttrType.attrInteger:
						sum = t.getIntFld(outer_merge_attr) + t.getIntFld(outer_in.length + inner_merge_attr);
						break;
					case AttrType.attrReal:
						sum = t.getFloFld(outer_merge_attr) + t.getFloFld(outer_in.length + inner_merge_attr);
						break;
					}
					t1.setFloFld(n_out_flds + 1, sum / 2);
					if(hf!=null)
						hf.insertRecord(t1.getTupleByteArray());
					else t1.print(topk_out_attr);
				}
				printDiskAccesses();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				free_buffer_pages(pageIds.length, pageIds);
				if (it != null)
					it.close();
			}
		} else {
			System.out.println("--------------------------------------------");
			System.out.println("Calling NRA-based Top-K Join");
			System.out.println("--------------------------------------------");
			getRelationAttrInfo(outerRelation);
			AttrType[] outer_in = curr_in.clone();
			short[] outer_strLens = curr_str_lens.clone();
			String[] outer_names = curr_attr_names.clone();
			getRelationAttrInfo(innerRelation);
			AttrType[] inner_in = curr_in.clone();
			short[] inner_strLens = curr_str_lens.clone();
			String[] inner_names = curr_attr_names.clone();
			
			ArrayList<IndexInfo> outer_indexes = getIndexInfo(outerRelation, outer_merge_attr);
			
			ArrayList<IndexInfo> inner_indexes = getIndexInfo(innerRelation, inner_merge_attr);
			
			String outer_index = "";
			String inner_index = "";
			
			for(IndexInfo iInfo:outer_indexes) {
				if(iInfo.getIndexType()==IndexType.B_Index&&iInfo.getClustered()==1) {
					outer_index = getIndexFileName(iInfo);
					break;
				}
			}
			for(IndexInfo iInfo:inner_indexes) {
				if(iInfo.getIndexType()==IndexType.B_Index&&iInfo.getClustered()==1) {
					inner_index = getIndexFileName(iInfo);
					break;
				}
				
			}
			
			if(outer_index.isEmpty()||inner_index.isEmpty()) {
				System.out.println("Clustered btree index does not exist.");
				return;
			}
			
			
			String output_names[] = new String[outer_in.length+inner_in.length];
			int p=0;
			for(String tmp:outer_names) {
				output_names[p++] = tmp;
			}
			for(String tmp:inner_names) {
				output_names[p++] = tmp;
			}
			int len_in1 = outer_in.length;
			int len_in2 = inner_in.length;
			int n_out_flds = len_in1 + len_in2;
			AttrType[] output_attr = new AttrType[n_out_flds];

			System.arraycopy(outer_in, 0, output_attr, 0, len_in1);
			System.arraycopy(inner_in, 0, output_attr, len_in1, len_in2);
			AttrType[] topk_out_attr = new AttrType[n_out_flds + 1];
			for (int i = 0; i < n_out_flds; i++) {
				topk_out_attr[i] = output_attr[i];
			}
			topk_out_attr[n_out_flds] = new AttrType(AttrType.attrReal);
			short output_str_lens[] = new short[outer_strLens.length + inner_strLens.length];
			int j = 0;
			for (short value : outer_strLens) {
				output_str_lens[j++] = value;
			}
			for (short value : inner_strLens) {
				output_str_lens[j++] = value;
			}
			PageId[] pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
			get_buffer_pages(pageIds.length, pageIds);
			PCounter.initialize();
			TopKNRAJoin topKNRAJoin = null;
			try {
			topKNRAJoin = new TopKNRAJoin(outer_in,outer_strLens,outer_join_attr,outer_merge_attr,
							inner_in,inner_strLens,inner_join_attr,inner_merge_attr,
							outer_index,inner_index,k,n_pages);

			Tuple t = topKNRAJoin.get_next();
			Heapfile outputTable = null;
			if(!outputTableName.isEmpty()) {
				outputTable = new Heapfile(outputTableName);
				addRelationAttrInfo(outputTableName, output_names, output_attr, output_str_lens);
			}
			Tuple t1 = new Tuple();
			t1.setHdr((short) (n_out_flds + 1), topk_out_attr, output_str_lens);
			t1 = new Tuple(t1.size());
			if(!outputTableName.isEmpty()) {
				while (t != null) {
					t1.setHdr((short) (n_out_flds + 1), topk_out_attr, output_str_lens);
					for (int i = 0; i < n_out_flds; i++) {
						switch (output_attr[i].attrType) {
						case AttrType.attrInteger:
							t1.setIntFld(i + 1, t.getIntFld(i + 1));
							break;
						case AttrType.attrReal:
							t1.setFloFld(i + 1, t.getFloFld(i + 1));
							break;
						case AttrType.attrString:
							t1.setStrFld(i + 1, t.getStrFld(i + 1));
							break;
						}
					}
					float sum = 0;
					switch (output_attr[outer_merge_attr].attrType) {
					case AttrType.attrInteger:
						sum = t.getIntFld(outer_merge_attr) + t.getIntFld(outer_in.length + inner_merge_attr);
						break;
					case AttrType.attrReal:
						sum = t.getFloFld(outer_merge_attr) + t.getFloFld(outer_in.length + inner_merge_attr);
						break;
					}
					t1.setFloFld(n_out_flds + 1, sum / 2);
					outputTable.insertRecord(t1.returnTupleByteArray());
					t = topKNRAJoin.get_next();
				}
				System.out.println("Output saved to new table. TableName = "+outputTableName);
			} else {
				System.out.println("Printing top k tuples");
				while (t != null) {
					System.out.println("--------------------------------------------");
					t1.setHdr((short) (n_out_flds + 1), topk_out_attr, output_str_lens);
					for (int i = 0; i < n_out_flds; i++) {
						switch (output_attr[i].attrType) {
						case AttrType.attrInteger:
							t1.setIntFld(i + 1, t.getIntFld(i + 1));
							break;
						case AttrType.attrReal:
							t1.setFloFld(i + 1, t.getFloFld(i + 1));
							break;
						case AttrType.attrString:
							t1.setStrFld(i + 1, t.getStrFld(i + 1));
							break;
						}
					}
					float sum = 0;
					switch (output_attr[outer_merge_attr].attrType) {
					case AttrType.attrInteger:
						sum = t.getIntFld(outer_merge_attr) + t.getIntFld(outer_in.length + inner_merge_attr);
						break;
					case AttrType.attrReal:
						sum = t.getFloFld(outer_merge_attr) + t.getFloFld(outer_in.length + inner_merge_attr);
						break;
					}
					t1.setFloFld(n_out_flds + 1, sum / 2);
					t1.print(topk_out_attr);
					t = topKNRAJoin.get_next();
				}
			}
			}catch(Exception e) {
				
			}finally {
				printDiskAccesses();
				free_buffer_pages(pageIds.length, pageIds);
				if(topKNRAJoin!=null)
					topKNRAJoin.close();
				
			}
			System.out.println("--------------------------------------------");
			System.out.println("NRA-based Top-K Join is done");
			System.out.println("--------------------------------------------");
		}
	}

	private static void join(String joinType, String outerRelation, int outer_attr, String innerRelation,
			int inner_attr, String operator, int n_pages, String outputRelation) throws Exception {
		getRelationAttrInfo(outerRelation);
		AttrType[] outer_in = curr_in.clone();
		short[] outer_strLens = curr_str_lens.clone();
		String[] outer_attr_names = curr_attr_names.clone();
		getRelationAttrInfo(innerRelation);
		AttrType[] inner_in = curr_in.clone();
		short[] inner_strLens = curr_str_lens.clone();
		String[] inner_attr_names = curr_attr_names.clone();
		CondExpr[] outFilter = new CondExpr[2];

		outFilter[0] = new CondExpr();
		outFilter[0].next = null;
		if (operator.equalsIgnoreCase("=")) {
			outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		} else if (operator.equalsIgnoreCase("<")) {
			outFilter[0].op = new AttrOperator(AttrOperator.aopLT);
		} else if (operator.equalsIgnoreCase("<=")) {
			outFilter[0].op = new AttrOperator(AttrOperator.aopLE);
		} else if (operator.equalsIgnoreCase(">")) {
			outFilter[0].op = new AttrOperator(AttrOperator.aopGT);
		} else if (operator.equalsIgnoreCase(">=")) {
			outFilter[0].op = new AttrOperator(AttrOperator.aopGE);
		}
		outFilter[1] = null;

		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outer_attr);

		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), inner_attr);

		CondExpr[] rightFilter = null;

		int len_in1 = outer_in.length;
		int len_in2 = inner_in.length;
		int n_out_flds = len_in1 + len_in2;
		FldSpec proj_list[] = new FldSpec[n_out_flds];
		AttrType[] output_attr = new AttrType[n_out_flds];
		FldSpec outer_proj_list[] = new FldSpec[len_in1];
		for (int i = 0; i < len_in1; i++) {
			proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			output_attr[i] = outer_in[i];
			outer_proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		for (int i = 0; i < len_in2; i++) {
			proj_list[i + len_in1] = new FldSpec(new RelSpec(RelSpec.innerRel), i + 1);
			output_attr[i + len_in1] = inner_in[i];
		}
		Iterator scan = null;
		PageId[] pageIds = null;
		try {
			switch (joinType) {
			case "HJ":
				ArrayList<IndexInfo> iInfo = getIndexInfo(innerRelation, inner_attr);
				int hash = 0;
				int splitPointer = 0;
				String indexName = "";
				boolean clustered = false;
				int indexType = IndexType.B_Index;
				if (operator.equalsIgnoreCase("=")) {
					for (IndexInfo indexInfo : iInfo) {
						if (indexInfo.getIndexType() == IndexType.Hash) {
							if (indexInfo.getClustered() == 1) {
								clustered = true;
								hash = indexInfo.getHash1();
								splitPointer = indexInfo.getSplitPointer();
								indexType = IndexType.Hash;
								indexName = getIndexFileName(indexInfo);
								break;
							}
							clustered = false;
							hash = indexInfo.getHash1();
							splitPointer = indexInfo.getSplitPointer();
							indexType = IndexType.Hash;
							indexName = getIndexFileName(indexInfo);
						}
					}
				}

				try {
					SystemDefs.JavabaseBM.flushAllPages();
				} catch (Exception e) {

				}
				pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
				get_buffer_pages(pageIds.length, pageIds);
				PCounter.initialize();
				if (indexName.isEmpty()) {
					scan = new HashJoins(outer_in, len_in1, outer_strLens, inner_in, len_in2, inner_strLens, n_pages,
							outerRelation, innerRelation, outFilter, rightFilter, proj_list, n_out_flds);
				} else {
					Iterator fileScan = new FileScan(outerRelation, outer_in, outer_strLens, (short) len_in1, len_in1,
							outer_proj_list, null);
					scan = new IndexNestedLoopsJoin(outer_in, len_in1, outer_strLens, inner_in, len_in2, inner_strLens,
							n_pages - 2, fileScan, innerRelation, indexType, clustered, hash, splitPointer, inner_attr,
							outer_attr, indexName, outFilter, rightFilter, proj_list, n_out_flds);
				}
				break;
			case "INLJ":
				iInfo = getIndexInfo(innerRelation, inner_attr);

				hash = 0;
				splitPointer = 0;
				indexName = "";
				clustered = false;
				indexType = IndexType.B_Index;
				if (operator.equalsIgnoreCase("=")) {
					for (IndexInfo indexInfo : iInfo) {
						if (indexInfo.getIndexType() == IndexType.Hash) {
							if (indexInfo.getClustered() == 1) {
								clustered = true;
								hash = indexInfo.getHash1();
								splitPointer = indexInfo.getSplitPointer();
								indexType = IndexType.Hash;
								indexName = getIndexFileName(indexInfo);
								break;
							}
							clustered = false;
							hash = indexInfo.getHash1();
							splitPointer = indexInfo.getSplitPointer();
							indexType = IndexType.Hash;
							indexName = getIndexFileName(indexInfo);
						}
					}
				}
				if (indexName.isEmpty()) {
					for (IndexInfo indexInfo : iInfo) {
						if (indexInfo.getIndexType() == IndexType.B_Index) {
							if (indexInfo.getClustered() == 1) {
								clustered = true;
								indexType = IndexType.B_Index;
								indexName = getIndexFileName(indexInfo);
								break;

							}
							clustered = false;
							indexType = IndexType.B_Index;
							indexName = getIndexFileName(indexInfo);
						}
					}
				}

				try {
					SystemDefs.JavabaseBM.flushAllPages();
				} catch (Exception e) {

				}
				pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
				get_buffer_pages(pageIds.length, pageIds);
				PCounter.initialize();

				Iterator fileScan = new FileScan(outerRelation, outer_in, outer_strLens, (short) len_in1, len_in1,
						outer_proj_list, null);

				if (indexName.isEmpty()) {
					scan = new NestedLoopsJoins(outer_in, len_in1, outer_strLens, inner_in, len_in2, inner_strLens,
							n_pages, fileScan, innerRelation, outFilter, rightFilter, proj_list, n_out_flds);
				} else {
					scan = new IndexNestedLoopsJoin(outer_in, len_in1, outer_strLens, inner_in, len_in2, inner_strLens,
							n_pages, fileScan, innerRelation, indexType, clustered, hash, splitPointer, inner_attr,
							outer_attr, indexName, outFilter, rightFilter, proj_list, n_out_flds);
				}
				break;
			case "NLJ":

				try {
					SystemDefs.JavabaseBM.flushAllPages();
				} catch (Exception e) {

				}
				pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
				get_buffer_pages(pageIds.length, pageIds);
				PCounter.initialize();

				fileScan = new FileScan(outerRelation, outer_in, outer_strLens, (short) len_in1, len_in1,
						outer_proj_list, null);
				scan = new NestedLoopsJoins(outer_in, len_in1, outer_strLens, inner_in, len_in2, inner_strLens, n_pages,
						fileScan, innerRelation, outFilter, rightFilter, proj_list, n_out_flds);
				break;
			case "SMJ":

				try {
					SystemDefs.JavabaseBM.flushAllPages();
				} catch (Exception e) {

				}
				pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
				get_buffer_pages(pageIds.length, pageIds);
				PCounter.initialize();

				Iterator fileScan1 = new FileScan(outerRelation, outer_in, outer_strLens, (short) len_in1, len_in1,
						outer_proj_list, null);
				Iterator fileScan2 = new FileScan(outerRelation, outer_in, outer_strLens, (short) len_in1, len_in1,
						outer_proj_list, null);
				scan = new SortMerge(outer_in, len_in1, outer_strLens, inner_in, len_in2, inner_strLens, outer_attr,
						outer_attr, inner_attr, inner_attr, n_pages, fileScan1, fileScan2, false, false,
						new TupleOrder(TupleOrder.Ascending), outFilter, proj_list, n_out_flds);
				break;
			}
			Tuple t = null;
			Heapfile ohf = null;
			short[] output_str_lens = new short[outer_strLens.length + inner_strLens.length];
			int k = 0;
			for (short tmp : outer_strLens)
				output_str_lens[k++] = tmp;
			for (short tmp : inner_strLens)
				output_str_lens[k++] = tmp;
			if (!outputRelation.isEmpty()) {
				String[] output_attr_names = new String[outer_attr_names.length + inner_attr_names.length];
				k = 0;
				for (String str : outer_attr_names)
					output_attr_names[k++] = str;
				for (String str : inner_attr_names)
					output_attr_names[k++] = str;
				addRelationAttrInfo(outputRelation, output_attr_names, output_attr, output_str_lens);
				ohf = new Heapfile(outputRelation);
			}
			int resultCnt = 0;
			while ((t = scan.get_next()) != null) {
				t.setHdr((short) output_attr.length, output_attr, output_str_lens);
				
				if (ohf != null) {
					ohf.insertRecord(t.getTupleByteArray());
				}else
					t.print(output_attr);
				resultCnt++;
			}
			System.out.println("counts::" + resultCnt);
			printDiskAccesses();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			free_buffer_pages(pageIds.length, pageIds);
			if (scan != null)
				scan.close();
		}

	}

	private static void computeSkyline(String type, String pref_list, String relationName, int n_pages,
			String outputRelation) {
		try {
			getRelationAttrInfo(relationName);
		} catch (Exception e1) {
			System.out.println("Relation does not exist");
			return;
		}
		AttrType[] in = curr_in.clone();
		short[] sSizes = curr_str_lens.clone();
		String[] attr_names = curr_attr_names.clone();
		String str[] = pref_list.split(",");
		int pref_list_length = str.length;
		int[] attr_list = new int[pref_list_length];
		int i = 0;
		for (String st : str) {
			attr_list[i++] = Integer.parseInt(st);
		}
		FldSpec[] projection = new FldSpec[in.length];
		for (i = 0; i < in.length; i++) {
			projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}

		if (type.equalsIgnoreCase("NLS")) {
			try {
				performNestedLoopsSky(in, sSizes, projection, attr_list, pref_list_length, relationName, n_pages,
						outputRelation, attr_names);
			} catch (IndexException | IOException e) {
				e.printStackTrace();
			}
		} else if (type.equalsIgnoreCase("BNLS")) {
			performBlockNestedSky(in, sSizes, projection, attr_list, pref_list_length, relationName, n_pages,
					outputRelation, attr_names);

		} else if (type.equalsIgnoreCase("SFS")) {
			try {
				performSortedSky(in, sSizes, projection, attr_list, pref_list_length, relationName, n_pages,
						outputRelation, attr_names);
			} catch (Exception e) {
				e.printStackTrace();
			}

		} else if (type.equalsIgnoreCase("BS")) {
			try {
				performBtreeSky(in, sSizes, projection, attr_list, pref_list_length, relationName, n_pages,
						outputRelation, attr_names);
			} catch (Exception e) {
				e.printStackTrace();
			}

		} else if (type.equalsIgnoreCase("BSS")) {
			try {
				performBtreeSortedSky(in, sSizes, projection, attr_list, pref_list_length, relationName, n_pages,
						outputRelation, attr_names);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void createTable(String fileName) throws Exception {
		if (sys == null) {
			System.out.println("Please open a database first.");
			return;
		}
		File file = new File(prefix_file_path + fileName + ".csv");
		BufferedReader br = new BufferedReader(new FileReader(file));
		int numberOfCols = Integer.parseInt(br.readLine().split(",")[0].trim());
		String attr_names[] = new String[numberOfCols];
		AttrType[] in = new AttrType[numberOfCols];
		int strnum = 0;
		int i = 0;
		while (i < numberOfCols) {
			String str[] = br.readLine().split(",");
			attr_names[i] = str[0];
			switch (str[1]) {
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
		Arrays.fill(strsizes, (short) GlobalConst.MAX_STR_LEN);
		addRelationAttrInfo(fileName, attr_names, in, strsizes);

		Heapfile hf = new Heapfile(fileName);

		String str = null;
		Tuple t = new Tuple();
		t.setHdr((short) numberOfCols, in, strsizes);
		t = new Tuple(t.size());
		t.setHdr((short) numberOfCols, in, strsizes);
		while ((str = br.readLine()) != null) {
			String attrs[] = str.split(",");

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
		System.out.println("Table " + fileName + " Created Successfully.");
	}

	private static void addRelationAttrInfo(String relationName, String[] attr_names, AttrType[] in, short[] strlens)
			throws Exception {
		getRelationAttrInfo(relationName);
		if (curr_in != null) {
			System.out.println(relationName + " already exists.");
			return;
		}
		Tuple t = new Tuple();
		t.setHdr((short) rel_attrs.length, rel_attrs, rel_str_lens);
		t = new Tuple(t.size());
		t.setHdr((short) rel_attrs.length, rel_attrs, rel_str_lens);
		t.setStrFld(1, relationName);
		t.setIntFld(2, in.length);
		t.setIntFld(3, strlens.length);
		relationHF.insertRecord(t.getTupleByteArray());
		t = new Tuple();
		t.setHdr((short) attr_attrs.length, attr_attrs, attr_str_lens);
		int k = 0;
		for (int i = 0; i < in.length; i++) {
			t = new Tuple(t.size());
			t.setHdr((short) attr_attrs.length, attr_attrs, attr_str_lens);
			t.setStrFld(1, relationName);
			t.setStrFld(2, attr_names[i]);
			t.setIntFld(3, in[i].attrType);
			t.setIntFld(4, in[i].attrType == AttrType.attrString ? strlens[k++] : 4);
			attrHF.insertRecord(t.getTupleByteArray());
		}

	}

	private static void getRelationAttrInfo(String relationName) throws Exception {
		curr_in = null;
		curr_attr_names = null;
		curr_str_lens = null;
		Scan scan = new Scan(relationHF);
		Tuple t = null;
		while ((t = scan.getNext(new RID())) != null) {
			t.setHdr((short) rel_attrs.length, rel_attrs, rel_str_lens);
			if (t.getStrFld(1).equalsIgnoreCase(relationName)) {
				scan.closescan();
				int num_col = t.getIntFld(2);
				int numstr = t.getIntFld(3);
				curr_in = new AttrType[num_col];
				curr_attr_names = new String[num_col];
				curr_str_lens = new short[numstr];
				scan = new Scan(new Heapfile(Catalogglobal.ATTRCATNAME));
				int i = 0;
				int j = 0;
				while ((t = scan.getNext(new RID())) != null) {
					t.setHdr((short) attr_attrs.length, attr_attrs, attr_str_lens);
					if (t.getStrFld(1).equalsIgnoreCase(relationName)) {
						curr_attr_names[i] = t.getStrFld(2);
						curr_in[i++] = new AttrType(t.getIntFld(3));
						if (curr_in[i - 1].attrType == AttrType.attrString) {
							curr_str_lens[j++] = (short) t.getIntFld(4);
						}
					}
				}
				scan.closescan();
			}

		}
		if (curr_in == null) {
			System.out.println("Relation " + relationName + " does not exist.");
		}
	}

	private static void addIndexInfo(String relationName, String indexName, int attr_num, int index_type,
			boolean clustered, int hash1, int numBuckets, int splitpointer) throws Exception {
		Tuple t = new Tuple();
		t.setHdr((short) ind_attrs.length, ind_attrs, ind_str_lens);
		t = new Tuple(t.size());
		t.setHdr((short) ind_attrs.length, ind_attrs, ind_str_lens);
		t.setStrFld(1, relationName);
		t.setIntFld(2, index_type);
		t.setIntFld(3, attr_num);
		t.setIntFld(4, clustered ? 1 : 0);
		t.setIntFld(5, hash1);
		t.setIntFld(6, numBuckets);
		t.setIntFld(7, splitpointer);
		indHF.insertRecord(t.getTupleByteArray());
	}

	private static ArrayList<IndexInfo> getIndexInfo(String relationName, int attrNum) throws Exception {
		ArrayList<IndexInfo> result = new ArrayList<>();
		IndexInfo indexInfo = null;
		Scan scan = new Scan(indHF);
		Tuple t = null;
		while ((t = scan.getNext(new RID())) != null) {
			t.setHdr((short) ind_attrs.length, ind_attrs, ind_str_lens);
			if (t.getStrFld(1).equalsIgnoreCase(relationName) && t.getIntFld(3) == attrNum) {
				indexInfo = new IndexInfo();
				indexInfo.setAttr_num(t.getIntFld(3));
				indexInfo.setClustered(t.getIntFld(4));
				indexInfo.setHash1(t.getIntFld(5));
				indexInfo.setIndexType(t.getIntFld(2));
				indexInfo.setNumBuckets(t.getIntFld(6));
				indexInfo.setRelationName(relationName);
				indexInfo.setSplitPointer(t.getIntFld(7));
				result.add(indexInfo);
			}
		}
		return result;
	}

	private static void createTableWithIndex(String fileName, int attr_num, int indexType) throws Exception {
		String indexFileName = "clst_";
		switch (indexType) {
		case IndexType.B_Index:
			indexFileName += "bt_";
			break;
		case IndexType.Hash:
			indexFileName += "hs_";
			break;
		}
		indexFileName += fileName + "_" + attr_num;
		String filepath = prefix_file_path + fileName + ".csv";
		File file = new File(filepath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		int numberOfCols = Integer.parseInt(br.readLine().split(",")[0].trim());
		String attr_names[] = new String[numberOfCols];
		AttrType[] in = new AttrType[numberOfCols];
		int strnum = 0;
		int i = 0;
		while (i < numberOfCols) {
			String str[] = br.readLine().split(",");
			attr_names[i] = str[0];
			switch (str[1]) {
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
		br.close();
		short[] strsizes = new short[strnum];
		Arrays.fill(strsizes, (short) GlobalConst.MAX_STR_LEN);
		addRelationAttrInfo(fileName, attr_names, in, strsizes);
		if (indexType == IndexType.Hash) {
			if (sys == null) {
				System.out.println("Please open a database first.");
				return;
			}

			ClusteredLinearHash clh = new ClusteredLinearHash(80, filepath, attr_num, fileName, indexFileName);
			addIndexInfo(fileName, indexFileName, attr_num, IndexType.Hash, true, clh.hash1, clh.numBuckets,
					clh.splitPointer);
		} else {
			// create a clustered btree index and update all the catalogs.
			ClusteredBtreeIndex cbi = new ClusteredBtreeIndex(fileName, filepath, indexFileName, attr_num);
			cbi.close();
			addIndexInfo(fileName, indexFileName, attr_num, IndexType.B_Index, true, 0, 0, 0);
		}
		System.out.println("Table with Index created Successfully");
	}

	private static String createIndex(String tableName, int attr_num, int indexType) throws Exception {
		String indexFileName = "uclst_";
		switch (indexType) {
		case IndexType.B_Index:
			indexFileName += "bt_";
			break;
		case IndexType.Hash:
			indexFileName += "hs_";
			break;
		}
		indexFileName += tableName + "_" + attr_num;
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		short[] strSizes = curr_str_lens.clone();

		if (indexType == IndexType.B_Index) {
			BTreeFile btf = null;
			try {
				btf = new BTreeFile(indexFileName, in[attr_num - 1].attrType,
						in[attr_num - 1].attrType == AttrType.attrString ? strSizes[0] : 4, 1);
			} catch (Exception e) {
				e.printStackTrace();
			}
			// Begin sorting process
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
					switch (in[attr_num - 1].attrType) {
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
				// Get next element to sort and add
				try {
					temp = scan.getNext(rid);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			btf.close();
			// close the file scan
			scan.closescan();
			addIndexInfo(tableName, indexFileName, attr_num, IndexType.B_Index, false, 0, 0, 0);
		} else {
			UnclusteredLinearHash ulh = new UnclusteredLinearHash(80, tableName, attr_num, strSizes, in, indexFileName);
			addIndexInfo(tableName, indexFileName, attr_num, IndexType.Hash, false, ulh.hash1, ulh.numBuckets,
					ulh.splitPointer);
		}
		System.out.println("Index Created Successfully.");
		sys.flushBuffer();
		return indexFileName;
	}

	private static void insertData(String tableName, String fileName) throws Exception {
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		short[] strLens = curr_str_lens.clone();
		File file = new File(prefix_file_path + fileName + ".csv");
		BufferedReader br = new BufferedReader(new FileReader(file));
		int numberOfCols = Integer.parseInt(br.readLine().split(",")[0].trim());
		while (numberOfCols-- > 0) {
			br.readLine();
		}
		ArrayList<IndexInfo> indices = new ArrayList<>();
		for (int i = 1; i <= in.length; i++) {
			indices.addAll(getIndexInfo(tableName, i));
		}
		IndexInfo clusteredIndex = null;
		for (IndexInfo indexInfo : indices) {
			if (indexInfo.getClustered() == 1) {
				clusteredIndex = indexInfo;
				break;
			}
		}
		if (clusteredIndex != null) {
			indices.remove(clusteredIndex);
		}
		ArrayList<UnclusteredLinearHash> uclhs = new ArrayList<>();
		ClusteredLinearHash clhs = null;
		ArrayList<BTreeFile> ubi = new ArrayList<>();
		ClusteredBtreeIndex bi = null;
		for (IndexInfo indexInfo : indices) {
			switch (indexInfo.getIndexType()) {
			case IndexType.B_Index:
				ubi.add(new BTreeFile(getIndexFileName(indexInfo)));
				break;
			case IndexType.Hash:
				uclhs.add(new UnclusteredLinearHash(getIndexFileName(indexInfo), indexInfo.getHash1(),
						indexInfo.getNumBuckets(), indexInfo.getSplitPointer(), indexInfo.getAttr_num(),
						indexInfo.getRelationName(), in, strLens));
				break;
			}
		}
		RID rid = null;
		Heapfile hf = null;
		if (clusteredIndex != null) {

			switch (clusteredIndex.getIndexType()) {
			case IndexType.B_Index:
				bi = new ClusteredBtreeIndex(clusteredIndex.getRelationName(), getIndexFileName(clusteredIndex),
						clusteredIndex.getAttr_num(), in, strLens);
				break;
			case IndexType.Hash:
				clhs = new ClusteredLinearHash(clusteredIndex.getRelationName(), getIndexFileName(clusteredIndex),
						clusteredIndex.getAttr_num(), clusteredIndex.getHash1(), clusteredIndex.getNumBuckets(),
						clusteredIndex.getSplitPointer(), in, strLens);
				break;
			}
		} else {
			hf = new Heapfile(tableName);
		}
		String str = null;
		Tuple t = new Tuple();
		t.setHdr((short) in.length, in, strLens);
		t = new Tuple(t.size());
		t.setHdr((short) in.length, in, strLens);
		while ((str = br.readLine()) != null) {
			String attrs[] = str.split(",");

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
			if (clusteredIndex != null) {
				switch (clusteredIndex.getIndexType()) {
				case IndexType.B_Index:
					rid = bi.insert(t);
					break;
				case IndexType.Hash:
					rid = clhs.insertIntoIndex(t);
					break;
				}
			} else {

				rid = hf.insertRecord(t.getTupleByteArray());
			}
			for (UnclusteredLinearHash ulh : uclhs) {
				ulh.insertInIndex(t, rid);
			}
			for (BTreeFile btf : ubi) {
				KeyClass key = null;
				String inInfo[] = btf.getIndexFileName().split("_");
				int attr_num = Integer.parseInt(inInfo[inInfo.length - 1]);
				switch (in[attr_num - 1].attrType) {
				case AttrType.attrInteger:
					btf.insert(new IntegerKey(t.getIntFld(attr_num)), rid);
					break;
				case AttrType.attrReal:
					btf.insert(new RealKey(t.getFloFld(attr_num)), rid);
					break;
				case AttrType.attrString:
					btf.insert(new StringKey(t.getStrFld(attr_num)), rid);
					break;
				}
				btf.insert(key, rid);
			}
		}
		if (clusteredIndex != null) {
			updateClstHashIndexInfo(clhs, tableName);
		}
		for (UnclusteredLinearHash ulh : uclhs) {
			updateUnclstHashIndexInfo(ulh, tableName);
		}
		if (bi != null)
			bi.close();
		for (BTreeFile btf : ubi) {
			btf.close();
		}
		br.close();
		sys.flushBuffer();
		System.out.println("Table " + fileName + " updated Successfully.");

	}

	private static void updateClstHashIndexInfo(ClusteredLinearHash clh, String relationName) throws Exception {
		Scan scan = new Scan(indHF);
		Tuple t = null;
		RID rid = new RID();
		while ((t = scan.getNext(rid)) != null) {
			t.setHdr((short) ind_attrs.length, ind_attrs, ind_str_lens);
			if (t.getStrFld(1).equalsIgnoreCase(relationName) && t.getIntFld(2) == IndexType.Hash
					&& t.getIntFld(3) == clh.indexField && t.getIntFld(4) == 1) {
				scan.closescan();
				t.setIntFld(5, clh.hash1);
				t.setIntFld(6, clh.numBuckets);
				t.setIntFld(7, clh.splitPointer);
				indHF.updateRecord(rid, t);
				break;
			}
		}
		scan.closescan();
	}

	private static void updateUnclstHashIndexInfo(UnclusteredLinearHash uclh, String relationName) throws Exception {
		Scan scan = new Scan(indHF);
		Tuple t = null;
		RID rid = new RID();
		while ((t = scan.getNext(rid)) != null) {
			t.setHdr((short) ind_attrs.length, ind_attrs, ind_str_lens);
			if (t.getStrFld(1).equalsIgnoreCase(relationName) && t.getIntFld(2) == IndexType.Hash
					&& t.getIntFld(3) == uclh.indexField && t.getIntFld(4) == 0) {
				scan.closescan();
				t.setIntFld(5, uclh.hash1);
				t.setIntFld(6, uclh.numBuckets);
				t.setIntFld(7, uclh.splitPointer);
				indHF.updateRecord(rid, t);
				break;
			}
		}
		scan.closescan();
	}

	private static void deleteData(String tableName, String fileName) throws Exception {
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		short[] strLens = curr_str_lens.clone();
		File file = new File(prefix_file_path + fileName + ".csv");
		BufferedReader br = new BufferedReader(new FileReader(file));
		int numberOfCols = Integer.parseInt(br.readLine().split(",")[0].trim());
		while (numberOfCols-- > 0) {
			br.readLine();
		}
		ArrayList<IndexInfo> indices = new ArrayList<>();
		for (int i = 1; i <= in.length; i++) {
			indices.addAll(getIndexInfo(tableName, i));
		}
		IndexInfo clusteredIndex = null;
		for (IndexInfo indexInfo : indices) {
			if (indexInfo.getClustered() == 1) {
				clusteredIndex = indexInfo;
				break;
			}
		}

//		IndexInfo unClusteredIndex = null;
		if (clusteredIndex != null) {
			indices.remove(clusteredIndex);

		} /*
			 * else { for (IndexInfo indexInfo : indices) { if (indexInfo.getClustered() ==
			 * 0) { unClusteredIndex = indexInfo; break; } } }
			 */
		int uncl = -1;
		ArrayList<UnclusteredLinearHash> uclhs = new ArrayList<>();
		ArrayList<BTreeFile> ubi = new ArrayList<>();
		for (IndexInfo indexInfo : indices) {
			switch (indexInfo.getIndexType()) {
			case IndexType.B_Index:
				ubi.add(new BTreeFile(getIndexFileName(indexInfo)));
				break;
			case IndexType.Hash:
				uclhs.add(new UnclusteredLinearHash(getIndexFileName(indexInfo), indexInfo.getHash1(),
						indexInfo.getNumBuckets(), indexInfo.getSplitPointer(), indexInfo.getAttr_num(),
						indexInfo.getRelationName(), in, strLens));
				break;
			}
		}
		if (uclhs.size() > 0) {
			uncl = IndexType.Hash;
		} else if (ubi.size() > 0) {
			uncl = IndexType.B_Index;
		}
		ClusteredLinearHash clhs = null;
		ClusteredBtreeIndex bi = null;

		Heapfile hf = null;
		if (clusteredIndex != null) {

			switch (clusteredIndex.getIndexType()) {
			case IndexType.B_Index:
				bi = new ClusteredBtreeIndex(clusteredIndex.getRelationName(), getIndexFileName(clusteredIndex),
						clusteredIndex.getAttr_num(), in, strLens);
				break;
			case IndexType.Hash:
				clhs = new ClusteredLinearHash(clusteredIndex.getRelationName(), getIndexFileName(clusteredIndex),
						clusteredIndex.getAttr_num(), clusteredIndex.getHash1(), clusteredIndex.getNumBuckets(),
						clusteredIndex.getSplitPointer(), in, strLens);
				break;
			}
		} else {
			hf = new Heapfile(tableName);
		}
		String str = null;
		Tuple t = new Tuple();
		t.setHdr((short) in.length, in, strLens);
		t = new Tuple(t.size());
		t.setHdr((short) in.length, in, strLens);
		ArrayList<RID> deletedTuples_final = new ArrayList<>();
		FldSpec[] projection = new FldSpec[in.length];
		for (int i = 0; i < in.length; i++) {
			projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		int j = 0;
		while ((str = br.readLine()) != null) {
			ArrayList<RID> deletedTuples = new ArrayList<>();
			String attrs[] = str.split(",");

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
			if (clusteredIndex != null) {
				switch (clusteredIndex.getIndexType()) {
				case IndexType.B_Index:
					deletedTuples.addAll(bi.delete(t));
					break;
				case IndexType.Hash:
					deletedTuples.addAll(clhs.deleteFromIndex(t));
					break;
				}
			} 
			else if (uncl != -1) {
				switch (uncl) {
				case IndexType.B_Index:
					CondExpr[] outFilter = new CondExpr[2];

					outFilter[0] = new CondExpr();
					outFilter[0].next = null;
					outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
					outFilter[1] = null;

					outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
					String nameParts[] = ubi.get(0).getIndexFileName().split("_");
					int fldNum = Integer.parseInt(nameParts[nameParts.length-1]);
					outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
							fldNum);
					switch (in[fldNum - 1].attrType) {
					case AttrType.attrInteger:
						outFilter[0].type2 = new AttrType(AttrType.attrInteger);
						outFilter[0].operand2.integer = t.getIntFld(fldNum);
						break;
					case AttrType.attrReal:
						outFilter[0].type2 = new AttrType(AttrType.attrReal);
						outFilter[0].operand2.real = t.getFloFld(fldNum);
						break;
					case AttrType.attrString:
						outFilter[0].type2 = new AttrType(AttrType.attrString);
						outFilter[0].operand2.string = t.getStrFld(fldNum);
						break;
					}
					IndexScan iScan = new IndexScan(new IndexType(IndexType.B_Index),
							tableName, ubi.get(0).getIndexFileName(), in, strLens,
							in.length, in.length, projection, outFilter, fldNum, false);
					RID del_rid = null;
					while ((del_rid = iScan.getNext()) != null) {
						Tuple tmp = hf.getRecord(del_rid);
						if (tmp == null)
							continue;
						tmp.setHdr((short) in.length, in, strLens);
						if (TupleUtils.Equal(tmp, t, in, in.length)) {
							deletedTuples.add(new RID(new PageId(del_rid.pageNo.pid),del_rid.slotNo));
						}
					}
					iScan.close();
					break;
				case IndexType.Hash:
					//String indexFile, AttrType[] in, short[] sSizes, int index_attr_no, String relationName, 
					//int t_attr_no,Tuple t, int hash, int splitPointer
					UnclusteredHashIndexScan uhis = new UnclusteredHashIndexScan(uclhs.get(0).directoryFile, in,
							strLens, uclhs.get(0).indexField, tableName, uclhs.get(0).indexField, t,
							uclhs.get(0).hash1, uclhs.get(0).splitPointer);
					RID del_rid1 = null;
					while ((del_rid1 = uhis.get_next(true)) != null) {
						Tuple tmp = hf.getRecord(del_rid1);
						if (tmp == null)
							continue;
						tmp.setHdr((short) in.length, in, strLens);
						if (TupleUtils.Equal(tmp, t, in, in.length))
							deletedTuples.add(new RID(new PageId(del_rid1.pageNo.pid),del_rid1.slotNo));
					}
					break;
				}
			} else {
				Scan scan = new Scan(hf);
				Tuple t3 = null;
				RID deleteRid = new RID();
				while ((t3 = scan.getNext(deleteRid)) != null) {
					t3.setHdr((short) in.length, in, strLens);
					if (TupleUtils.Equal(t, t3, in, in.length)) {
						deletedTuples.add(new RID(new PageId(deleteRid.pageNo.pid), deleteRid.slotNo));
					}
				}
				scan.closescan();
			}
			j+=deletedTuples.size();
			//deletedTuples_final.addAll(deletedTuples);
			if (clusteredIndex == null) {
				for (RID ridd : deletedTuples) {
					try {
						hf.deleteRecord(ridd);
					}catch(Exception e) {
					}
				}
			}
			for (UnclusteredLinearHash uclh : uclhs) {
				for (RID ridd : deletedTuples) {
					uclh.deleteFromIndex(t, ridd);
				}
			}
			for (BTreeFile btf : ubi) {
				KeyClass key = null;
				String str1[] = btf.getIndexFileName().split("_");
				int fldNo = Integer.parseInt(str1[str1.length - 1]);
				switch (in[fldNo - 1].attrType) {
				case AttrType.attrInteger:
					key = new IntegerKey(t.getIntFld(fldNo));
					break;
				case AttrType.attrReal:
					key = new RealKey(t.getFloFld(fldNo));
					break;
				case AttrType.attrString:
					key = new StringKey(t.getStrFld(fldNo));
					break;
				}
				for (RID ridd : deletedTuples) {
					System.out.println(btf.Delete(key, ridd));
					System.out.println(ridd.slotNo + " " + ridd.pageNo + " deleted from index");
				}
			}
			/*
			 * for (IndexInfo indexInfo : indices) { switch (indexInfo.getIndexType()) {
			 * case IndexType.B_Index: BTreeFile ub = new
			 * BTreeFile(getIndexFileName(indexInfo));
			 * 
			 * for (RID ridd : deletedTuples) { System.out.println(ub.Delete(key, ridd));
			 * System.out.println(ridd.slotNo+" "+ridd.pageNo+" deleted from index"); }
			 * ub.close(); break; case IndexType.Hash: UnclusteredLinearHash uclh = new
			 * UnclusteredLinearHash(getIndexFileName(indexInfo), indexInfo.getHash1(),
			 * indexInfo.getNumBuckets(), indexInfo.getSplitPointer(),
			 * indexInfo.getAttr_num(), indexInfo.getRelationName(), in, strLens); for (RID
			 * ridd : deletedTuples) { uclh.deleteFromIndex(t, ridd); }
			 * updateUnclstHashIndexInfo(uclh, tableName); break; } }
			 */

		}
		
		//System.out.println(j+" size");

		for (BTreeFile btf : ubi) {
			btf.close();
		}
		for (UnclusteredLinearHash uclh : uclhs) {
			updateUnclstHashIndexInfo(uclh, tableName);
		}
		if (bi != null)
			bi.close();
		if (br != null)
			br.close();
		if (clhs != null)
			updateClstHashIndexInfo(clhs, tableName);
		System.out.println("Deleted successfully.");
	}

	public static void reIndex(String relationName, List<RID> oldRIDs, List<RID> newRIDs) throws Exception {
		getRelationAttrInfo(relationName);
		AttrType[] in = curr_in.clone();
		short[] strLens = curr_str_lens.clone();
		Heapfile relation = new Heapfile(relationName);
		ArrayList<IndexInfo> indices = new ArrayList<>();
		for (int i = 1; i <= in.length; i++) {
			indices.addAll(getIndexInfo(relationName, i));
		}
		IndexInfo clusteredIndex = null;
		for (IndexInfo indexInfo : indices) {
			if (indexInfo.getClustered() == 1) {
				clusteredIndex = indexInfo;
				break;
			}
		}
		if (clusteredIndex != null) {
			indices.remove(clusteredIndex);
		}
		for (IndexInfo iInfo : indices) {
			switch (iInfo.getIndexType()) {
			case IndexType.B_Index:
				BTreeFile ub = new BTreeFile(getIndexFileName(iInfo));
				for (int i = 0; i < oldRIDs.size(); i++) {
					RID newRid = newRIDs.get(i);
					RID oldRid = oldRIDs.get(i);
					Tuple t = relation.getRecord(newRid);
					t.setHdr((short) in.length, in, strLens);
					KeyClass key = null;
					switch (in[iInfo.getAttr_num() - 1].attrType) {
					case AttrType.attrInteger:
						key = new IntegerKey(t.getIntFld(iInfo.getAttr_num()));
						break;
					case AttrType.attrReal:
						key = new RealKey(t.getFloFld(iInfo.getAttr_num()));
						break;
					case AttrType.attrString:
						key = new StringKey(t.getStrFld(iInfo.getAttr_num()));
						break;
					}
					ub.Delete(key, oldRid);
					ub.insert(key, newRid);
				}
				ub.close();
				break;
			case IndexType.Hash:
				UnclusteredLinearHash uclh = new UnclusteredLinearHash(getIndexFileName(iInfo), iInfo.getHash1(),
						iInfo.getNumBuckets(), iInfo.getSplitPointer(), iInfo.getAttr_num(), iInfo.getRelationName(),
						in, strLens);
				for (int i = 0; i < newRIDs.size(); i++) {
					RID newRid = newRIDs.get(i);
					RID oldRid = oldRIDs.get(i);
					Tuple t = relation.getRecord(newRid);
					t.setHdr((short) in.length, in, strLens);
					uclh.deleteFromIndex(t, oldRid);
					uclh.insertInIndex(t, newRid);
				}
				break;
			}
		}

	}

	private static void updateData(String dbName) throws Exception {
		/*
		 * getRelationAttrInfo(tableName); AttrType[] in = curr_in.clone(); short[]
		 * strLens = curr_str_lens.clone(); Heapfile hf = new Heapfile(tableName); Scan
		 * scan = new Scan(hf); Tuple t = null; RID rid = new RID(); while ((t =
		 * scan.getNext(rid)) != null) { // t.setHdr((short)in.length, in,strLens);
		 * scan.closescan(); Page page = new Page();
		 * SystemDefs.JavabaseBM.pinPage(rid.pageNo, page, false); HFPage hfp = new
		 * HFPage(page); RID currPage = new RID(); for (currPage = hfp.firstRecord();
		 * currPage != null; currPage = hfp.nextRecord(currPage)) { if
		 * (currPage.equals(rid)) { t = hfp.returnRecord(currPage); t.setHdr((short)
		 * in.length, in, strLens); t.setIntFld(2, t.getIntFld(2) + 1);
		 * System.out.println("Update Done"); break; } }
		 * SystemDefs.JavabaseBM.unpinPage(rid.pageNo, true); break; } scan.closescan();
		 */

		String dbpath = prefix_file_path + System.getProperty("user.name") + ".minibase." + dbName;

		String remove_cmd = "/bin/rm -rf ";
		String remove_dbcmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		sys = new SystemDefs(dbpath, 100000, GlobalConst.NUMBUF, "Clock");
	}

	private static void printTable(String tableName) throws Exception {
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		short[] strLens = curr_str_lens.clone();
		String[] attr_names = curr_attr_names.clone();
		Tuple t = null;
		Scan scan = new Scan(new Heapfile(tableName));
		System.out.println("====" + tableName + "====");
		System.out.println(Arrays.toString(attr_names));
		int j = 0;
		try {
			while ((t = scan.getNext(new RID())) != null) {
				t.setHdr((short) attr_names.length, in, strLens);
				t.print(in);
				j++;
			}
		}catch(Exception e) {
			
		}
		System.out.println("Total tuples:: " + j);
	}

	private static String getIndexFileName(IndexInfo indexInfo) {
		String indexName = indexInfo.getClustered() == 0 ? "uclst" : "clst";
		indexName += indexInfo.getIndexType() == IndexType.B_Index ? "_bt_" : "_hs_";
		indexName += indexInfo.getRelationName() + "_";
		indexName += indexInfo.getAttr_num();
		return indexName;
	}

	private static void outputIndex(String tableName, int attr_num) throws Exception {
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		short[] strLens = curr_str_lens.clone();

		ArrayList<IndexInfo> indices = getIndexInfo(tableName, attr_num);
		for (IndexInfo iInfo : indices) {
			String indexName = getIndexFileName(iInfo);
			System.out.println("====printing the index file::" + indexName + "====");
			switch (iInfo.getIndexType()) {
			case IndexType.B_Index:
				switch (iInfo.getClustered()) {
				case 0:
					BTreeFile btree = new BTreeFile(indexName);
					BT.printAllLeafPages(btree.getHeaderPage());
					btree.close();
					break;
				case 1:
					ClusteredBtreeIndex clusteredBtreeIndex = new ClusteredBtreeIndex(tableName, indexName, attr_num,
							in, strLens);
					clusteredBtreeIndex.printCBtree();
					clusteredBtreeIndex.close();
					break;
				}
				break;
			case IndexType.Hash:
				switch (iInfo.getClustered()) {
				case 0:
//					UnclusteredHashIndexScan uhs = new UnclusteredHashIndexScan(indexName, in, strLens, attr_num, tableName, false);
//					Tuple t = null;
//					int splitPoint = iInfo.getSplitPointer();
//					int hash1 = iInfo.getHash1();
//					int hash2 = 2*hash1;
//					System.out.println(hash1+" "+splitPoint+" "+iInfo.getNumBuckets());
//					while((t=uhs.get_next())!=null) {
//						t.setHdr((short)in.length, in, strLens);
//						String data = t.getStrFld(1);
//						long hash = 7;
//						for (int i = 0; i < data.length(); i++) {
//						    hash = hash*11 + data.charAt(i);
//						}
//						hash%=hash1;
//						System.out.print(data+" "+hash+" tuple:: ");
//						t.print(in);
//					}
					UnclusteredLinearHash ulh = new UnclusteredLinearHash(indexName, iInfo.getHash1(),
							iInfo.getNumBuckets(), iInfo.getSplitPointer(), attr_num, tableName, in, strLens);
					ulh.printIndex();
					break;
				case 1:
//					ClusteredHashIndexScan chis = new ClusteredHashIndexScan(indexName, in, strLens, attr_num);
//					Tuple t = null;
//					int j = 0;
//					while ((t = chis.get_next()) != null) {
//						j++;
//					}
					ClusteredLinearHash clh = new ClusteredLinearHash(tableName, indexName, attr_num, iInfo.getHash1(),
							iInfo.getNumBuckets(), iInfo.getSplitPointer(), in, strLens);
					clh.printIndex();
					//System.out.println("total tuples::" + j);
					break;
				}
				break;
			}
		}
	}

	private static void groupby(String type, String agg_type, int agg_attr_num, String attr_list, String tableName,
			int n_pages, String outputTable) throws Exception {
		getRelationAttrInfo(tableName);
		AttrType[] in = curr_in.clone();
		int len_in1 = in.length;
		short[] strLens = curr_str_lens.clone();
		String str[] = attr_list.split(",");
		int[] agg_list = new int[str.length];
		int i = 0;
		for (String st : str) {
			agg_list[i++] = Integer.parseInt(st);
		}
		AggType agg_tp = null;
		switch (agg_type) {
		case "max":
			agg_tp = new AggType(AggType.aggMax);
			break;
		case "min":
			agg_tp = new AggType(AggType.aggMin);
			break;
		case "avg":
			agg_tp = new AggType(AggType.aggAvg);
			break;
		case "sky":
			agg_tp = new AggType(AggType.aggSky);
			break;
		}
		short[] ssizes = null;
		if (in[agg_attr_num - 1].attrType == AttrType.attrString) {
			ssizes = new short[1];
			ssizes[0] = strLens[0];
		}
		int number_cols = agg_list.length + 1;
		AttrType[] result_in = new AttrType[number_cols];
		result_in[0] = in[agg_attr_num - 1];
		for (i = 1; i < number_cols; i++) {
			result_in[i] = agg_tp.agg_type == AggType.aggSky ? in[agg_list[i - 1] - 1]
					: new AttrType(AttrType.attrReal);
		}
		Heapfile ohf = null;
		if (!outputTable.isEmpty()) {
			String attr_names[] = new String[number_cols];
			attr_names[0] = curr_attr_names[0];
			for (i = 1; i < number_cols; i++) {
				attr_names[i] = agg_tp.agg_type == AggType.aggSky ? curr_attr_names[agg_list[i - 1] - 1]
						: agg_type + "_" + i;
			}
			addRelationAttrInfo(outputTable, attr_names, result_in, ssizes);
			ohf = new Heapfile(outputTable);
		}

		ArrayList<IndexInfo> iInfo = getIndexInfo(tableName, agg_attr_num);
		boolean indexExists = false;
		boolean clustered = false;
		String indexName = "";
		switch (type) {
		case "sort":

			for (IndexInfo indexInfo : iInfo) {
				if (indexInfo.getIndexType() == IndexType.B_Index) {
					if (indexInfo.getClustered() == 1) {
						indexExists = true;
						indexName = getIndexFileName(indexInfo);
						break;
					}
				}
			}

			try {
				SystemDefs.JavabaseBM.flushAllPages();
			} catch (Exception e) {

			}
			PageId[] pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
			get_buffer_pages(pageIds.length, pageIds);
			PCounter.initialize();
			GroupBywithSort gbs = null;
			try {
				gbs = new GroupBywithSort(in, len_in1, strLens, tableName, agg_attr_num, agg_list, agg_tp, n_pages,
						indexExists, indexName);
				Tuple t = null;

				while ((t = gbs.get_next()) != null) {
					t.setHdr((short) number_cols, result_in, ssizes);
					if (ohf != null)
						ohf.insertRecord(t.getTupleByteArray());
					else t.print(result_in);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (gbs != null) {
					gbs.close();
				}
				free_buffer_pages(pageIds.length, pageIds);
			}
			printDiskAccesses();
			break;
		case "hash":

			for (IndexInfo indexInfo : iInfo) {
				if (indexInfo.getIndexType() == IndexType.Hash) {
					indexExists = true;
					if (indexInfo.getClustered() == 1) {
						clustered = true;
						indexName = getIndexFileName(indexInfo);
						break;
					}
					clustered = false;
					indexName = getIndexFileName(indexInfo);
				}
			}

			try {
				SystemDefs.JavabaseBM.flushAllPages();
			} catch (Exception e) {

			}
			PCounter.initialize();
			if (!indexExists) {
				indexName = createIndex(tableName, agg_attr_num, IndexType.Hash);
			}
			int r_tmp = PCounter.rcounter;
			int w_tmp = PCounter.wcounter;
			pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
			get_buffer_pages(pageIds.length, pageIds);
			PCounter.rcounter = r_tmp;
			PCounter.wcounter = w_tmp;
			GroupBywithHash gbh = null;
			try {
				gbh = new GroupBywithHash(in, len_in1, strLens, tableName, agg_attr_num, agg_list, agg_tp, n_pages,
						clustered, indexName);
				Tuple t = null;
				while ((t = gbh.get_next()) != null) {
					t.setHdr((short) number_cols, result_in, ssizes);
					if (ohf != null)
						ohf.insertRecord(t.getTupleByteArray());
					else t.print(result_in);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				r_tmp = PCounter.rcounter;
				w_tmp = PCounter.wcounter;
				free_buffer_pages(pageIds.length, pageIds);
				PCounter.rcounter = r_tmp;
				PCounter.wcounter = w_tmp;
				if (gbh != null)
					gbh.close();

			}
			printDiskAccesses();
			break;
		}
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
		System.out.println(
				"9> skyline NLS/BNLS/SFS/BS/BSS {att1,att2,...,atth} tablename npages [mater outputtablename]");
		System.out.println(
				"10> groupby sort/hash max/min/avg/sky g_attr_no {att1,att2,...,atth} tablename npages [mater outputtablename]");
		System.out.println(
				"11> join NLJ/SMJ/INLJ/HJ OTABLENAME O_ATT_NO ITABLENAME I_ATT_NO OP(\"=\"/\"<=\"/\"<\"/\">=\"/\">\") npages [mater outputtablename]");
		System.out.println(
				"12> TOPKJOIN hash/nra k OTABLENAME O_J_att_no o_M_att_no Itablename I_J_att_no I_M_att_no npages [mater outputtablename]");
		System.out.println("13> Type \"EXIT\" to terminate the session\n");
	}

	static void openDB(String dbName) {
		String dbpath = prefix_file_path + System.getProperty("user.name") + ".minibase." + dbName;
		File file = new File(dbpath);
		if (file.exists()) {
			sys = new SystemDefs(dbpath, 0, GlobalConst.NUMBUF, "Clock");
			System.out.println("Opened an existing database");
		} else {
			sys = new SystemDefs(dbpath, 100000, GlobalConst.NUMBUF, "Clock");
			System.out.println("Created a new database");
		}
		try {
			relationHF = new Heapfile(Catalogglobal.RELCATNAME);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1) {
			System.out.println("Error creating relation catalog");
		}
		try {
			indHF = new Heapfile(Catalogglobal.INDEXCATNAME);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1) {
			System.out.println("Error creating index catalog");
		}
		try {
			attrHF = new Heapfile(Catalogglobal.ATTRCATNAME);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e1) {
			System.out.println("Error creating attribute catalog");
		}
		try {
			String attr_names[] = { "RELATIONNAME", "ATTR_COUNT", "STR_COUNT" };
			addRelationAttrInfo(Catalogglobal.RELCATNAME, attr_names, rel_attrs, rel_str_lens);
			String i_attr_names[] = { "RELATIONNAME", "INDEXTYPE", "INDEX_ATTR", "IS_CLUSTERED", "HASH_VALUE",
					"NUM_BUCKETS", "SPLIT_POINTER" };
			addRelationAttrInfo(Catalogglobal.INDEXCATNAME, i_attr_names, ind_attrs, ind_str_lens);
			String at_attr_names[] = { "RELATIONNAME", "ATTR_NAME", "ATTR_TYPE", "ATTR_SIZE" };
			addRelationAttrInfo(Catalogglobal.ATTRCATNAME, at_attr_names, attr_attrs, attr_str_lens);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Print Skyline tuples
	static void printTuple(int tuple_count, AttrType[] in, Tuple t) throws Exception {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println("Skyline tuple # " + tuple_count);
		System.out.println("-----------------------------------");
		t.print(in);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
	}

	// Start NestedLoopsSky
	static void performNestedLoopsSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages, String outputRelation, String[] attr_names)
			throws IndexException, IOException {
		// For scanning outer loop
		sys.flushBuffer();

		PageId[] pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
		try {
			get_buffer_pages(pageIds.length, pageIds);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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
			Heapfile ohf = null;
			if (!outputRelation.isEmpty()) {
				addRelationAttrInfo(outputRelation, attr_names, in, Ssizes);
				ohf = new Heapfile(outputRelation);
			}
			while ((nestedLoopSkyline = nlSky.get_next()) != null) {
				if (ohf != null) {
					ohf.insertRecord(nestedLoopSkyline.getTupleByteArray());
				}else {
					printTuple(tuple_count, in, nestedLoopSkyline);
					
				}
				tuple_count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// Print the read / write count on disk
			printDiskAccesses();
			if (nlScan != null)
				nlScan.close();
			if (nlSky != null)
				nlSky.close();
			try {
				free_buffer_pages(pageIds.length, pageIds);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// Start BlockNestedLoopSky
	static void performBlockNestedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages, String outputRelation, String[] attr_names) {
		FileScan am2 = null;
		BlockNestedLoopSky sky2 = null;
		sys.flushBuffer();

		PageId[] pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
		try {
			get_buffer_pages(pageIds.length, pageIds);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
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
				Heapfile ohf = null;
				if (!outputRelation.isEmpty()) {
					addRelationAttrInfo(outputRelation, attr_names, in, Ssizes);
					ohf = new Heapfile(outputRelation);
				}
				while ((t1 = sky2.get_next()) != null) {
					
					if (ohf != null) {
						ohf.insertRecord(t1.getTupleByteArray());
					}else printTuple(tuple_count, in, t1);
					tuple_count++;
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				sky2.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			printDiskAccesses();
			try {
				free_buffer_pages(pageIds.length, pageIds);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// Print the read / write count on disk
	}

	// Start BtreeSortedSky
	static void performBtreeSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages, String outputRelation, String[] attr_names)
			throws Exception {

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

		PageId[] pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
		get_buffer_pages(pageIds.length, pageIds);

		PCounter.initialize();
		Iterator sc = new BTreeSortedSky(in, (short) in.length, Ssizes, null, relationName, pref_list, pref_list_length,
				"BTreeIndex", n_pages);
		Tuple t1 = null;
		int tuple_count = 1;
		try {
			Heapfile ohf = null;
			if (!outputRelation.isEmpty()) {
				addRelationAttrInfo(outputRelation, attr_names, in, Ssizes);
				ohf = new Heapfile(outputRelation);
			}
			while ((t1 = sc.get_next()) != null) {
				
				if (ohf != null) {
					ohf.insertRecord(t1.getTupleByteArray());
				}else printTuple(tuple_count, in, t1);
				tuple_count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			printDiskAccesses();
			sc.close();
			if (btf != null) {
				System.out.println("File deleted");
				btf.destroyFile();
				SystemDefs.JavabaseDB.delete_file_entry("BTreeIndex");
			}
			free_buffer_pages(pageIds.length, pageIds);
		}
		// Print the read / write count on disk
	}

	// Start SortFistSky
	static void performSortedSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages, String outputRelation, String[] attr_names)
			throws Exception {
		sys.flushBuffer();

		PageId[] pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
		get_buffer_pages(pageIds.length, pageIds);
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
			Heapfile ohf = null;
			if (!outputRelation.isEmpty()) {
				addRelationAttrInfo(outputRelation, attr_names, in, Ssizes);
				ohf = new Heapfile(outputRelation);
			}
			while ((t1 = sc.get_next()) != null) {
				tuple_count++;
				if (ohf != null) {
					ohf.insertRecord(t1.getTupleByteArray());
				}else printTuple(tuple_count, in, t1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			printDiskAccesses();
			sc.close();
			free_buffer_pages(pageIds.length, pageIds);
		}
		// Print the read / write count on disk
	}

	// Start BtreeSky
	static void performBtreeSky(AttrType[] in, short[] Ssizes, FldSpec[] projection, int[] pref_list,
			int pref_list_length, String relationName, int n_pages, String outputRelation, String[] attr_names)
			throws Exception {
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
		PageId[] pageIds = new PageId[GlobalConst.NUMBUF - n_pages];
		get_buffer_pages(pageIds.length, pageIds);
		PCounter.initialize();
//Create BTreeSky iterator and store tuples to buffer
//Reserves certain pages for file creation and scanners	  

		BTreeSky btScan = new BTreeSky(in, (short) in.length, Ssizes, null, relationName, pref_list, pref_list_length,
				indexFiles, n_pages);

// Print Skyline Tuples
		Tuple t1 = null;
		int tuple_count = 1;
		try {
			Heapfile ohf = null;
			if (!outputRelation.isEmpty()) {
				addRelationAttrInfo(outputRelation, attr_names, in, Ssizes);
				ohf = new Heapfile(outputRelation);
			}
			while ((t1 = btScan.get_next()) != null) {
				tuple_count++;
				if (ohf != null) {
					ohf.insertRecord(t1.getTupleByteArray());
				}else printTuple(tuple_count, in, t1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			printDiskAccesses();
			for (BTreeFile ub : btf) {
				ub.destroyFile();
				SystemDefs.JavabaseDB.delete_file_entry(ub.getIndexFileName());
				System.out.println("Deleted file :: " + ub.getIndexFileName());
			}
			free_buffer_pages(pageIds.length, pageIds);
			btScan.close();
		}
// Print the read / write count on disk

	}

	// Print read write on disk
	static void printDiskAccesses() {
		System.out.println("Read Count: " + PCounter.rcounter);

		System.out.println("Write Count: " + PCounter.wcounter);
	}

	private static void get_buffer_pages(int n_pages, PageId[] PageIds) throws Exception {
		Page pgptr = new Page();
		PageId pgid = null;

		for (int i = 0; i < n_pages; i++) {

			pgid = SystemDefs.JavabaseBM.newPage(pgptr, 1);
			PageIds[i] = new PageId(pgid.pid);

		}
	}

	private static void free_buffer_pages(int n_pages, PageId[] PageIds) throws Exception {
		for (int i = 0; i < n_pages; i++) {
			SystemDefs.JavabaseBM.freePage(PageIds[i]);
		}
	}
}
