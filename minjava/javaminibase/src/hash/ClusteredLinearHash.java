package hash;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import iterator.TupleUtils;

public class ClusteredLinearHash {

	private static int hash1 = 4;
	private static int hash2 = 8;
	
	private static final String STR = "STR";
	private static final String INT = "INT";
	public static final String prefix= "TMP_HASH_CLST_";
	public static final String directoryPrefix = "dir_hs_";
	
	private int current_tuples=0;
	private int targetUtilization=80;
	private int tuple_threshold=0;
	private int splitPointer = 0;
	private int number_of_tuples_in_a_page = 0;
	private int totalTuples = 0;
	private int numBuckets=hash1;
	
	
	private AttrType[] _in;
	private int indexField;
	private String fileName;
	private String filePath;
	private short numberOfCols;
	private short[] strSizes;
	private Heapfile relation;
	
	
	private AttrType keyPageAttr[];
	private short[] keyPageStrlens;
	private short keyPageColNum = 2;
	

	private AttrType PageAttr[];
	private short pageColNum = 1;
	
	
	private String indexFileName;
	private Heapfile hashDirectory;
	private AttrType dirAttr[];
	private short[] dirStrlens;
	private short dirColNum = 1;

	public void initialize() {
		hash1 = 4;
		hash2 = 8;
		targetUtilization=80;
		splitPointer=0;
		numBuckets=hash1;
		current_tuples=0;
	}
	
	public ClusteredLinearHash(int utilization, String filepath, int attr_num, String filename, String indexFileName) throws Exception {
		initialize();
		this.fileName = filename;
		this.targetUtilization = utilization;
		this.indexField = attr_num;
		this.filePath = filepath;
		relation = new Heapfile(fileName);
		this.indexFileName = indexFileName;
		hashDirectory = new Heapfile(this.indexFileName);
		
		PageAttr = new AttrType[pageColNum];
		PageAttr[0] = new AttrType(AttrType.attrInteger);
		
		dirAttr = new AttrType[dirColNum];
		dirAttr[0] = new AttrType(AttrType.attrString);
		
		dirStrlens= new short[1];
		dirStrlens[0] = GlobalConst.MAX_NAME + 2;
		
		clusterRecordsFromfile();
		
		System.out.println(numBuckets+" :: "+splitPointer);
//		for(int i=0;i<hash2;i++) {
//			Heapfile f=  new Heapfile(prefix+i);
//			Scan scan = new Scan(f);
//			t = null;
//			System.out.println("Printing heapfile::"+prefix+i);
//			while((t=scan.getNext(new RID()))!=null) {
//				t.setHdr((short)numberOfCols, _in, strSizes);
//				t.print(_in);
//			}
//		}
		createClusteredFile(0,numBuckets);
//		Scan scan = new Scan(new Heapfile(fileName));
//		Tuple t = null;
//		int k=0;
//		RID rid = new RID();
//		while((t=scan.getNext(rid))!=null) {
//			t.setHdr((short)numberOfCols, _in, strSizes);
//			t = new Tuple(t);
//			t.setHdr((short)numberOfCols, _in, strSizes);
//			t.print(_in);
//			int tmp = calculateHashValueForTuple(t, false);
//			if(tmp<splitPointer)
//				tmp = calculateHashValueForTuple(t, true);
//			System.out.println(tmp+" :: "+rid.pageNo.pid);
//			k++;
//		}
//		System.out.println(k);
	}
	
	private void clusterRecordsFromfile() throws Exception {
		// Enter the path for data file
		File file = new File(filePath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		this.numberOfCols = (short) Integer.parseInt(br.readLine().trim());

		if (indexField > numberOfCols || indexField < 1) {
			br.close();
			throw new Exception("Clustered Attribute is out of the range");
		}
		String str = "";

		_in = new AttrType[numberOfCols];
		int numStr = 0;
		for (int i = 0; i < numberOfCols; i++) {
			str = br.readLine();
			String attrInfo[] = str.split("\\t");
			if (attrInfo[1].equalsIgnoreCase(STR)) {
				numStr++;
				_in[i] = new AttrType(AttrType.attrString);
			} else if (attrInfo[1].equalsIgnoreCase(INT)) {
				_in[i] = new AttrType(AttrType.attrInteger);
			}else {
				_in[i] = new AttrType(AttrType.attrReal);
			}
		}

		strSizes = new short[numStr];
		Arrays.fill(strSizes, (short) 30);
		
		keyPageAttr = new AttrType[keyPageColNum];
		keyPageAttr[1] = new AttrType(AttrType.attrString);
		
		
		keyPageAttr[0] = new AttrType(_in[indexField-1].attrType);

		if(_in[indexField-1].attrType==AttrType.attrString) {
			keyPageStrlens = new short[2];
			keyPageStrlens[0]=strSizes[0];
			keyPageStrlens[1]=GlobalConst.MAX_NAME+2;
		}else {
			keyPageStrlens = new short[1];
			keyPageStrlens[0]=GlobalConst.MAX_NAME+2;
		}
		

//		FldSpec[] projection = new FldSpec[numberOfCols];
//
//		for (int i = 0; i < numberOfCols; i++) {
//			projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
//		}

		Tuple t = new Tuple();
		try {
			t.setHdr((short) numberOfCols, _in, strSizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		int size = t.size();
		t.setHdr((short)1, PageAttr, null);
		number_of_tuples_in_a_page = (GlobalConst.MAX_SPACE - HFPage.DPFIXED) / (t.size() + HFPage.SIZE_OF_SLOT);
		setTotalTuplesAndThreshold();

		t = new Tuple(size);
		try {
			t.setHdr((short) numberOfCols, _in, strSizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		try {
			while ((str = br.readLine()) != null) {
				String attrs[] = str.split("\\t");

				int k = 1;

				for (String attr : attrs) {
					attr = attr.trim();
					if (attr.equals(""))
						continue;
					switch (_in[k - 1].attrType) {
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
				clusterTuplesByHashing(t);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private void clusterTuplesByHashing(Tuple t) throws Exception {
		int hashValue = calculateHashValueForTuple(t, false);
		if (hashValue < splitPointer)
			hashValue = calculateHashValueForTuple(t, true);
		if (!(hashValue < 0)) {
			try {
				Heapfile f = new Heapfile(prefix + hashValue);
				f.insertRecord(t.getTupleByteArray());
				current_tuples++;
				if (tuple_threshold == current_tuples) {
					Heapfile sf = new Heapfile("TempHash");
					f = new Heapfile(prefix + splitPointer);
					Scan scan = new Scan(f);
					Tuple t1 = null;
					RID rid = new RID();
					while ((t1 = scan.getNext(rid)) != null) {
						t1.setHdr((short) numberOfCols, _in, strSizes);
						int newBucket = calculateHashValueForTuple(t1, true);
						if (newBucket != splitPointer) {
							sf.insertRecord(tupleFromRid(rid).getTupleByteArray());
							new Heapfile(prefix + newBucket).insertRecord(t1.getTupleByteArray());
						}
					}
					scan.closescan();
					scan = new Scan(sf);
					while ((t1 = scan.getNext(rid)) != null) {
						f.deleteRecord(ridFromTuple(t1));
					}
					sf.deleteFile();
					increamentSplit();
					setTotalTuplesAndThreshold();

				}
			} catch (InvalidSlotNumberException | InvalidTupleSizeException | SpaceNotAvailableException
					| HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
				e.printStackTrace();
			}
		} else {
			throw new Exception("Problem computing hash value for certain tuple");
		}
	}
	
	
	private int calculateMaxCapacityForKeyPair() throws Exception{
		return (number_of_tuples_in_a_page*targetUtilization)/100;
	}
	
	private String getHashBucketName(int hash) {
		return directoryPrefix+fileName+hash;
	}
	
	private String getHashBucketInnerHeapfileName(Tuple t, int hash) throws Exception{
		String key = "";
		switch(_in[indexField-1].attrType) {
		case AttrType.attrInteger:
			key+=t.getIntFld(indexField);
			break;
		case AttrType.attrString:
			key+=t.getStrFld(indexField);
			break;
		case AttrType.attrReal:
			key+=t.getFloFld(indexField);
			break;
		}
		return directoryPrefix+fileName+hash+key;
	}
	
	private void createClusteredFile(int startHash, int endHash) throws Exception {
		
		for(int i=startHash;i<endHash;i++) {
			Heapfile f=  new Heapfile(prefix+i);
			Heapfile df = new Heapfile(getHashBucketName(i));
			Scan scan = new Scan(f);
			Tuple t = null;
			boolean isEmpty=true;
			while((t=scan.getNext(new RID()))!=null) {
				isEmpty = false;
				t.setHdr(numberOfCols, _in, strSizes);
				Scan bucketScan = new Scan(df);
				Tuple pageTuple = null;
				PageId currentPageId = new PageId();
				while((pageTuple=bucketScan.getNext(new RID()))!=null) {
					pageTuple.setHdr(keyPageColNum, keyPageAttr, keyPageStrlens);
					if(TupleUtils.CompareTupleWithTuple(keyPageAttr[0], t, indexField, pageTuple, 1)==0) {
						break;
					}
				}
				bucketScan.closescan();
				int flag = -1;
				if(pageTuple!=null) {
					flag = 0;
					bucketScan = new Scan(new Heapfile(pageTuple.getStrFld(2)));
					Tuple t2 = null;
					while((t2=bucketScan.getNext(new RID()))!=null) {
						t2.setHdr(pageColNum, PageAttr, null);
						currentPageId = new PageId(t2.getIntFld(1));
						Page page = new Page();
						pinPage(currentPageId, page, false);
						HFPage dataPage = new HFPage(page);
						if(dataPage.available_space()>=t.size()) {
							unpinPage(currentPageId, false);
							flag = 1;
							break;
						}
						unpinPage(currentPageId, false);
					}
					bucketScan.closescan();
				}
				if(flag==1){
					relation.insertRecord(t.getTupleByteArray(), currentPageId);
				}else {
					Page page = new Page();
					PageId pid = newPage(page, 1);
					relation.insertRecord(t.getTupleByteArray(),pid);
					String name = "";
					if(flag==-1) {
						name = getHashBucketInnerHeapfileName(t,i);
						df.insertRecord(keyTupleFromTuple(t,name).getTupleByteArray(),calculateMaxCapacityForKeyPair());
					}else {
						name = pageTuple.getStrFld(2);
					}
					new Heapfile(name).insertRecord(TupleFromPageId(pid).getTupleByteArray());
				}
			}
			if(!isEmpty) {
				Tuple tup = new Tuple();
				tup.setHdr(dirColNum, dirAttr, dirStrlens);
				tup=new Tuple(tup.size());
				tup.setHdr(dirColNum, dirAttr, dirStrlens);
				tup.setStrFld(1, df.getName());
				hashDirectory.insertRecord(tup.getTupleByteArray());
			}
			scan.closescan();
			f.deleteFile();
		}
	}
	
	
	private RID ridFromTuple(Tuple t1) throws Exception {
		AttrType[] _in = new AttrType[2];
	    for(int i = 0; i < 2; i++)
	      _in[i] = new AttrType(AttrType.attrInteger);
	    try {
	      t1.setHdr((short) 2, _in, null);
	    } catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      e.printStackTrace();
	    }
	    RID rid = new RID();
	    rid.slotNo = t1.getIntFld(2);
		rid.pageNo = new PageId(t1.getIntFld(1));
		return rid;
	}
	
	private Tuple tupleFromRid(RID rid) throws Exception {
		AttrType[] _in = new AttrType[2];
	    for(int i = 0; i < 2; i++)
	      _in[i] = new AttrType(AttrType.attrInteger);
	    Tuple t = new Tuple();
	    try {
	      t.setHdr((short) 2, _in, null);
	    } catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      e.printStackTrace();
	    }
	
	    int size = t.size();
	    t = new Tuple(size);
	    try {
	      t.setHdr((short) 2, _in, null);
	    } catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      e.printStackTrace();
	    }
	    t.setIntFld(1, rid.pageNo.pid);
	    t.setIntFld(2, rid.slotNo);
	    return t;
	}
	
	private void increamentSplit() {
		splitPointer++;
		if(splitPointer==hash1+1) {
			hash1 = hash2;
			hash2 = 2*hash1;
			splitPointer=0;
		}
	}
	
	private void setTotalTuplesAndThreshold() {
		totalTuples = number_of_tuples_in_a_page*numBuckets;
		tuple_threshold = (targetUtilization*totalTuples)/100;
		numBuckets++;
		
	}
	
	private int calculateHashValueForTuple(Tuple t,boolean reHash) throws Exception{
		int hashValue = -1;
		switch(_in[indexField-1].attrType) {
			case AttrType.attrInteger:
				hashValue = calculateHash(t.getIntFld(indexField),reHash);
				break;
			case AttrType.attrString:
				hashValue = calculateHash(t.getStrFld(indexField),reHash);
				break;
			case AttrType.attrReal:
				hashValue = calculateHash(t.getFloFld(indexField), reHash);
				break;
			default:
				break;
		}
		return hashValue;
	}
	
	private int calculateHash(String data,boolean reHash) {
		int hash = 7;
		for (int i = 0; i < data.length(); i++) {
		    hash = hash*11 + data.charAt(i);
		}
		if(!reHash)
			return hash%hash1;
		return hash%hash2;
	}
	private int calculateHash(float data,boolean reHash) {
		if(!reHash)
			return ((int)(data*100))%hash1;
		return ((int)data*100)%hash2;
	}
	
	private int calculateHash(int data,boolean reHash) {
		if(!reHash)
			return data%hash1;
		return data%hash2;
	}
	
	
	private Tuple keyTupleFromTuple(Tuple t1,String name) throws Exception{
		Tuple t = new Tuple();
		t.setHdr(keyPageColNum, keyPageAttr, keyPageStrlens);
		t = new Tuple(t.size());
		t.setHdr(keyPageColNum, keyPageAttr, keyPageStrlens);
		switch(_in[indexField-1].attrType) {
		case AttrType.attrInteger:
			t.setIntFld(1, t1.getIntFld(indexField));
			break;
		case AttrType.attrString:
			t.setStrFld(1, t1.getStrFld(indexField));
			break;
		case AttrType.attrReal:
			t.setFloFld(1, t1.getFloFld(indexField));
			break;
		}
		t.setStrFld(2, name);
		return t;
	}
	
	private Tuple TupleFromPageId(PageId pageId) throws Exception{
		Tuple t = new Tuple();
		t.setHdr(pageColNum, PageAttr, null);
		t = new Tuple(t.size());
		t.setHdr(pageColNum, PageAttr, null);
		t.setIntFld(1,  pageId.pid);
		return t;
	}
	
	
	
	/**
	   * short cut to access the pinPage function in bufmgr package.
	   * @see bufmgr.pinPage
	   */
	  private void pinPage(PageId pageno, Page page, boolean emptyPage)
	    throws HFBufMgrException {
	    
	    try {
	      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
	    }
	    catch (Exception e) {
	      throw new HFBufMgrException(e,"Heapfile.java: pinPage() failed");
	    }
	    
	  } // end of pinPage

	  /**
	   * short cut to access the unpinPage function in bufmgr package.
	   * @see bufmgr.unpinPage
	   */
	  private void unpinPage(PageId pageno, boolean dirty)
	    throws HFBufMgrException {

	    try {
	      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
	    }
	    catch (Exception e) {
	      throw new HFBufMgrException(e,"Heapfile.java: unpinPage() failed");
	    }

	  } // end of unpinPage

	  private PageId newPage(Page page, int num)
	    throws HFBufMgrException {

	    PageId tmpId = new PageId();

	    try {
	      tmpId = SystemDefs.JavabaseBM.newPage(page,num);
	      unpinPage(tmpId, true);
	    }
	    catch (Exception e) {
	      throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
	    }

	    return tmpId;

	  }
}
