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

	public  int hash1 = 4;
	private  int hash2 = 8;
	
	public final String prefix= "TMP_HASH_CLST_";
	public final String directoryPrefix = "dir_hs_";
	
	private int current_pages=0;
	private int targetUtilization=80;
	private int tuple_threshold=0;
	public int splitPointer = 0;
	private int number_of_tuples_in_a_page = 0;
	private int totalTuples = 0;
	public int numBuckets=hash1;
	
	
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
		current_pages=0;
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
		
//		System.out.println(numBuckets+" :: "+splitPointer+" :: "+hash1);
	}
	
	public ClusteredLinearHash(String relationName, String indexFileName, int attr_num, int hash1, int numBuckets, 
    		int splitPointer,AttrType[] in, short[] strLens) throws Exception{
		this._in = in.clone();
		this.strSizes = strLens.clone();
		
		this.numberOfCols = (short)_in.length;
		this.targetUtilization = 100;
		this.hash1 = hash1;
		this.numBuckets = numBuckets-1;
		this.splitPointer = splitPointer;
		
		
		this.fileName = relationName;
		this.indexField = attr_num;
		relation = new Heapfile(fileName);
		this.current_pages = relation.getPgCnt();

		this.indexFileName = indexFileName;
		hashDirectory = new Heapfile(this.indexFileName);
		
		PageAttr = new AttrType[pageColNum];
		PageAttr[0] = new AttrType(AttrType.attrInteger);
		
		dirAttr = new AttrType[dirColNum];
		dirAttr[0] = new AttrType(AttrType.attrString);
		
		dirStrlens= new short[1];
		dirStrlens[0] = GlobalConst.MAX_NAME + 2;
		
		keyPageAttr = new AttrType[keyPageColNum];
		keyPageAttr[1] = new AttrType(AttrType.attrInteger);
		keyPageAttr[0] = new AttrType(_in[indexField-1].attrType);

		if(_in[indexField-1].attrType==AttrType.attrString) {
			keyPageStrlens = new short[1];
			keyPageStrlens[0]=strSizes[0];
		}
		
		Tuple t = new Tuple();
		t.setHdr((short)1, PageAttr, null);
		number_of_tuples_in_a_page = (GlobalConst.MAX_SPACE - HFPage.DPFIXED) / (t.size() + HFPage.SIZE_OF_SLOT);
		setTotalTuplesAndThreshold();
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
			if (attrInfo[1].equalsIgnoreCase(GlobalConst.STR)) {
				numStr++;
				_in[i] = new AttrType(AttrType.attrString);
			} else if (attrInfo[1].equalsIgnoreCase(GlobalConst.INT)) {
				_in[i] = new AttrType(AttrType.attrInteger);
			}else {
				_in[i] = new AttrType(AttrType.attrReal);
			}
		}

		strSizes = new short[numStr];
		Arrays.fill(strSizes, (short) 30);
		
		keyPageAttr = new AttrType[keyPageColNum];
		keyPageAttr[1] = new AttrType(AttrType.attrInteger);
		keyPageAttr[0] = new AttrType(_in[indexField-1].attrType);

		if(_in[indexField-1].attrType==AttrType.attrString) {
			keyPageStrlens = new short[1];
			keyPageStrlens[0]=strSizes[0];
		}		
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
				insertIntoIndex(t);
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
	private RID insertIntoPage(Tuple t, Heapfile bucket) throws Exception{
		Scan scan = new Scan(bucket);
		Tuple t1 = null;
		PageId result = null;
		while((t1 = scan.getNext(new RID()))!=null) {
			t1.setHdr(keyPageColNum, keyPageAttr, keyPageStrlens);
			if(TupleUtils.CompareTupleWithTuple(_in[indexField-1], t, indexField, t1, 1)==0) {
				result = new PageId(t1.getIntFld(2));
				break;
			}
		}
		scan.closescan();
		if(result==null) {
			result = newPage(new Page(), 1);
			bucket.insertRecord(keyTupleFromTuple(t, result.pid).getTupleByteArray());
		}else {
			while(result!=null) {
				Page page = new Page();
				pinPage(result, page, false);
				HFPage hfp = new HFPage(page);
				if(hfp.available_space()>=t.size()) {
					unpinPage(result, false);
					break;
				}
				int tmp = hfp.getNextPage().pid;
				if(tmp==GlobalConst.INVALID_PAGE) {
					result = newPage(new Page(), 1);
					hfp.setNextPage(result);
					Page nextPage = new Page();
					pinPage(result, nextPage, true);
					HFPage nhfp = new HFPage();
					nhfp.init(result, nextPage);
					nhfp.setPrevPage(hfp.getCurPage());
					unpinPage(result, true);
					unpinPage(hfp.getCurPage(), true);
					break;
				}else {
					unpinPage(result, false);
					result = new PageId(tmp);
				}
			}
		}
		return relation.insertRecord(t.getTupleByteArray(), result);
	}
	
	private boolean setupDirectory(String name) throws Exception{
    	Tuple t = null;
    	t = new Tuple();
		t.setHdr(dirColNum, dirAttr,dirStrlens);
		t = new Tuple(t.size());
		t.setHdr(dirColNum, dirAttr,dirStrlens);
   		t.setStrFld(1, name);
   		hashDirectory.insertRecord(t.getTupleByteArray());
   	    return true;
    }
	
	public RID insertIntoIndex(Tuple t) throws Exception{
		int hashValue = calculateHashValueForTuple(t, false);
		if (hashValue < splitPointer)
			hashValue = calculateHashValueForTuple(t, true);
		if (!(hashValue < 0)) {
			try {
				Heapfile bucket = new Heapfile(getHashBucketName(hashValue));
				if(bucket.isEmpty()) {
					setupDirectory(getHashBucketName(hashValue));
				}
				RID insert_rid = insertIntoPage(t, bucket);
				if (tuple_threshold == current_pages) {
					Heapfile sf = new Heapfile("TempHash");
					Heapfile f = new Heapfile(getHashBucketName(splitPointer));
					Scan scan = new Scan(f);
					Tuple t1 = null;
					RID rid = new RID();
					while ((t1 = scan.getNext(rid)) != null) {
						t1.setHdr(keyPageColNum, keyPageAttr, keyPageStrlens);
						int newBucket = calculateHashValueFromKeyTuple(t1, true);
						if (newBucket != splitPointer) {
							sf.insertRecord(tupleFromRid(rid).getTupleByteArray());
							Heapfile tmp = new Heapfile(getHashBucketName(newBucket));
							if(tmp.isEmpty())
								setupDirectory(getHashBucketName(newBucket));
							tmp.insertRecord(t1.getTupleByteArray());
						}
					}
					scan.closescan();
					scan = new Scan(sf);
					while ((t1 = scan.getNext(rid)) != null) {
						f.deleteRecord(ridFromTuple(t1));
					}
					scan.closescan();
					sf.deleteFile();
					increamentSplit();
					setTotalTuplesAndThreshold();

				}
				return insert_rid;
			} catch (InvalidSlotNumberException | InvalidTupleSizeException | SpaceNotAvailableException
					| HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
				e.printStackTrace();
			}
		} else {
			throw new Exception("Problem computing hash value for certain tuple");
		}
		return null;
	}
	
	/***
	 * This method assumes that the record is not deleted from the datafile,
	 * it just removes the record from index and update index if necessary
	 * @param t
	 * @param rid
	 * @throws Exception
	 */
	public void deleteFromIndex(Tuple t, RID rid) throws Exception {
		int recCnt = relation.getRecCnt(rid.pageNo);
		if(recCnt==1) {
			int hash = calculateHashValueForTuple(t, false);
			if(hash<splitPointer) {
				hash = calculateHashValueForTuple(t, true);
			}
			String bucketName = getHashBucketName(hash);
			Heapfile hf = new Heapfile(bucketName);
			Scan scan = new Scan(hf);
			Tuple t1 = null;
			RID rid1 = new RID();
			PageId header = null;
			while((t1=scan.getNext(rid1))!=null) {
				t1.setHdr(keyPageColNum, keyPageAttr, keyPageStrlens);
				if(TupleUtils.CompareTupleWithTuple(_in[indexField-1], t, indexField, t1, 1)==0) {
					header = new PageId(t1.getIntFld(2));
					break;
				}
				
			}
			if(header!=null) {
				if(header.pid==rid.pageNo.pid) {
					hf.deleteRecord(rid1);
				}else {
					Page page = new Page();
					pinPage(header, page, false);
					HFPage hfp = new HFPage(page);
					while(hfp.getNextPage().pid!=GlobalConst.INVALID_PAGE &&
							hfp.getNextPage().pid!=rid.pageNo.pid) {
						unpinPage(header, false);
						header = hfp.getNextPage();
						if(header.pid == GlobalConst.INVALID_PAGE)break;
						pinPage(header,page,false);
						hfp = new HFPage(page);
					}
					if(header.pid!=GlobalConst.INVALID_PAGE) {
						PageId prev = hfp.getCurPage();
						Page nextPage = new Page();
						pinPage(hfp.getNextPage(),nextPage,false);
						HFPage nhfp = new HFPage(nextPage);
						hfp.setNextPage(nhfp.getNextPage());
						unpinPage(nhfp.getCurPage(),false);
						if(hfp.getNextPage().pid!=GlobalConst.INVALID_PAGE) {
							pinPage(hfp.getNextPage(),nextPage,false);
							nhfp = new HFPage(hfp);
							nhfp.setPrevPage(prev);
							unpinPage(nhfp.getCurPage(), true);
						}
						unpinPage(hfp.getCurPage(), true);
					}
				}
			}
		}
	}
	
	public void printIndex() throws Exception{
		Scan scan = new Scan(hashDirectory);
		Tuple t = null;
		while((t = scan.getNext(new RID()))!=null) {
			t.setHdr(dirColNum, dirAttr, dirStrlens);
			String bucketName = t.getStrFld(1);
			String offset = directoryPrefix+fileName;
			System.out.println("====Printing the bucket with hash value::"+bucketName.substring(offset.length(), bucketName.length())+"====");
			Scan innerScan = new Scan(new Heapfile(bucketName));
			while((t=innerScan.getNext(new RID()))!=null) {
				t.setHdr(keyPageColNum, keyPageAttr, keyPageStrlens);
				String result = "[ key = ";
				switch(keyPageAttr[0].attrType) {
				case AttrType.attrInteger:
					result+=t.getIntFld(1);
					break;
				case AttrType.attrReal:
					result+=t.getFloFld(1);
					break;
				case AttrType.attrString:
					result+=t.getStrFld(1);
					break;
				}
				result+=", pageIds = [ ";
				PageId header = new PageId(t.getIntFld(2));
				while(header.pid!=GlobalConst.INVALID_PAGE) {
					result+=header.pid+", ";
					Page page = new Page();
					pinPage(header, page, false);
					HFPage hfp  =new HFPage(page);
					header = hfp.getNextPage();
					unpinPage(hfp.getCurPage(), false);
				}
				result+="]";
				System.out.println(result);
			}
			innerScan.closescan();
		}
		scan.closescan();
	}
	
	private String getHashBucketName(int hash) {
		return directoryPrefix+fileName+hash;
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
	private int calculateHashValueFromKeyTuple(Tuple t,boolean reHash) throws Exception{
		int hashValue = -1;
		switch(_in[indexField-1].attrType) {
			case AttrType.attrInteger:
				hashValue = calculateHash(t.getIntFld(1),reHash);
				break;
			case AttrType.attrString:
				hashValue = calculateHash(t.getStrFld(1),reHash);
				break;
			case AttrType.attrReal:
				hashValue = calculateHash(t.getFloFld(1), reHash);
				break;
			default:
				break;
		}
		return hashValue;
	}
	
	private int calculateHash(String data,boolean reHash) {
		long hash = 7;
		for (int i = 0; i < data.length(); i++) {
		    hash = hash*11 + data.charAt(i);
		}
		if(!reHash)
			return (int)(hash%hash1);
		return (int)(hash%hash2);
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
	
	
	private Tuple keyTupleFromTuple(Tuple t1,int pageId) throws Exception{
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
		t.setIntFld(2, pageId);
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
	      current_pages++;
	      unpinPage(tmpId, false);
	    }
	    catch (Exception e) {
	      throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
	    }

	    return tmpId;

	  }
}
