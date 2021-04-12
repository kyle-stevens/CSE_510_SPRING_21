package hash;

import global.*;
import heap.*;
import iterator.TupleUtils;

public class UnclusteredLinearHash {

    public int hash1 = 4;
    private int hash2 = 8;
    private int current_tuples=0;
    private int targetUtilization=80;
    private int tuple_threshold=0;
    private String prefix= "unclst_buc_";
    private String key_file_prefix = "unclst_key_";
    public int splitPointer = 0;
    public int number_of_tuples_in_a_page = 0;
    private int totalTuples = 0;
    public int numBuckets=hash1;
    
    private AttrType[] _in;
    private int indexField;
    
    private String fileName;

    private short numberOfCols;
    private short[] strSizes;
    
    private Heapfile relation;
    
    
    private AttrType keyPageAttr[];
    
    private AttrType dir_f_Attr[];
    private short[] dir_f_str_lens;
    
    private AttrType keyFileAttr[];
    private short[] keyFileStrLens;
    
    private String directoryFile;


    public UnclusteredLinearHash(int utilization, String filename, int attr_num, short[] strSizes, AttrType[] in, String indexfilename) throws Exception {
        
    	this.fileName = filename;
        this.targetUtilization = utilization<=0?80:utilization;
        this.indexField = attr_num;
        this.directoryFile = indexfilename;
        
        this._in = in.clone();
        this.strSizes = strSizes.clone();
        this.numberOfCols = (short)_in.length;
        
        relation = new Heapfile(fileName);
        
        prefix += fileName+"_"+attr_num+"_";
        key_file_prefix += fileName+"_"+attr_num+"_";
        
        dir_f_Attr = new AttrType[1];
        dir_f_Attr[0] = new AttrType(AttrType.attrString);
        dir_f_str_lens = new short[1];
        dir_f_str_lens[0] = GlobalConst.MAX_NAME+2;
        
        keyFileAttr = new AttrType[2];
        keyFileAttr[0] = _in[attr_num-1];
        keyFileAttr[1] = new AttrType(AttrType.attrString);
        
        if(_in[attr_num-1].attrType==AttrType.attrString) {
        	keyFileStrLens = new short[2];
        	keyFileStrLens[0] = strSizes[0];
        	keyFileStrLens[1] = GlobalConst.MAX_NAME+2;
        }else {
        	keyFileStrLens = new short[1];
        	keyFileStrLens[0] = GlobalConst.MAX_NAME+2;
        	
        }
        
        keyPageAttr = new AttrType[2];
        keyPageAttr[0] = new AttrType(AttrType.attrInteger);
        keyPageAttr[1] = new AttrType(AttrType.attrInteger);
        
        
        Tuple t = new Tuple();
        t.setHdr((short)2, keyPageAttr, null);
        int size = t.size();

        number_of_tuples_in_a_page = (GlobalConst.MAX_SPACE - HFPage.DPFIXED) / (size + HFPage.SIZE_OF_SLOT);
        
        hash1 = (int)((relation.getRecCnt()*100.0)/(utilization*number_of_tuples_in_a_page))+1;
        hash2 = 2*hash1;
        numBuckets=hash1;
        setTotalTuplesAndThreshold();
        createIndex();
    }
    
    public UnclusteredLinearHash(String indexFile, int hash1, int numBuckets, 
    		int splitPointer, int attr_no, String relationName,
    		AttrType[] in, short[] strLens) throws Exception{
    	this.hash1 = hash1;
    	this.hash2 = 2*hash1;
    	this.numBuckets = numBuckets-1;
    	this.splitPointer = splitPointer;        
    	this.indexField = attr_no;
        this.directoryFile = indexFile;
        this._in = in.clone();
        this.strSizes = strLens.clone();
        this.numberOfCols = (short)_in.length;
        fileName = relationName;
        relation = new Heapfile(fileName);
        current_tuples = relation.getRecCnt();
        
        prefix += fileName+"_"+indexField+"_";
        key_file_prefix += fileName+"_"+indexField+"_";
        
        dir_f_Attr = new AttrType[1];
        dir_f_Attr[0] = new AttrType(AttrType.attrString);
        dir_f_str_lens = new short[1];
        dir_f_str_lens[0] = GlobalConst.MAX_NAME+2;
        
        keyFileAttr = new AttrType[2];
        keyFileAttr[0] = _in[indexField-1];
        keyFileAttr[1] = new AttrType(AttrType.attrString);
        
        if(_in[indexField-1].attrType==AttrType.attrString) {
        	keyFileStrLens = new short[2];
        	keyFileStrLens[0] = strSizes[0];
        	keyFileStrLens[1] = GlobalConst.MAX_NAME+2;
        }else {
        	keyFileStrLens = new short[1];
        	keyFileStrLens[0] = GlobalConst.MAX_NAME+2;
        	
        }
        
        keyPageAttr = new AttrType[2];
        keyPageAttr[0] = new AttrType(AttrType.attrInteger);
        keyPageAttr[1] = new AttrType(AttrType.attrInteger);
        
        
        Tuple t = new Tuple();
        t.setHdr((short)2, keyPageAttr, null);
        int size = t.size();

        number_of_tuples_in_a_page = (GlobalConst.MAX_SPACE - HFPage.DPFIXED) / (size + HFPage.SIZE_OF_SLOT);
        targetUtilization=100;
        setTotalTuplesAndThreshold();
        
    }
    
    private int calculateMaxCapacityForKeyPair() throws Exception{
		return (number_of_tuples_in_a_page*targetUtilization)/100;
	}
    
    private void createIndex() throws Exception{
    	Scan scan = new Scan(relation);
    	Tuple t = null;
    	RID rid = new RID();
    	while((t=scan.getNext(rid))!=null) {
    		insertInIndex(t,rid);
    	}
    }
    
    public void deleteFromIndex(Tuple t, RID rid) throws Exception{
    	t.setHdr(numberOfCols, _in, strSizes);
		t = new Tuple(t);
		t.setHdr(numberOfCols, _in, strSizes);
		int hash = calculateHashValueForTuple(t,false);
		if(hash<splitPointer) {
			hash = calculateHashValueForTuple(t, true);
		}
		String name = getHashBucketInnerHeapfileName(t, hash);
		RID rid1 = new RID();
		Heapfile hf = new Heapfile(name);
		Scan scan = new Scan(hf);
		while((t = scan.getNext(rid1))!=null) {
			if(rid.equals(ridFromTuple(t))) {
				scan.closescan();
				hf.deleteRecord(rid1);
				return;
			}
		}
    }
    
    private void setupDirectory(String name) throws Exception{
    	Heapfile directory = new Heapfile(directoryFile);
    	Scan scan = new Scan(directory);
    	Tuple t = null;
    	while((t=scan.getNext(new RID()))!=null) {
    		t.setHdr((short)1, dir_f_Attr, dir_f_str_lens);
       		String curStr = t.getStrFld(1);
       		if(curStr.equalsIgnoreCase(name)) {
       			scan.closescan();
       			return;
       		}
    	}
    	scan.closescan();
    	t = new Tuple();
		t.setHdr((short)1, dir_f_Attr, dir_f_str_lens);
		t = new Tuple(t.size());
   		t.setHdr((short)1, dir_f_Attr, dir_f_str_lens);
   		t.setStrFld(1, name);
   	    directory.insertRecord(t.getTupleByteArray());
    }
    
    public void insertInIndex(Tuple t, RID rid) throws Exception{
    	t.setHdr(numberOfCols, _in, strSizes);
		t = new Tuple(t);
		t.setHdr(numberOfCols, _in, strSizes);
		int hash = calculateHashValueForTuple(t,false);
		if(hash<splitPointer) {
			hash = calculateHashValueForTuple(t, true);
		}
		if(hash>=0) {
			setupDirectory(prefix+hash);
			Heapfile hf = new Heapfile(prefix+hash);
			Scan bucketScan = new Scan(hf);
			Tuple t2 = null;
			String str = "";
			while((t2 = bucketScan.getNext(new RID()))!=null) {
				t2.setHdr((short)2, keyFileAttr, keyFileStrLens);
				if(TupleUtils.CompareTupleWithTuple(_in[indexField-1], t, indexField, t2, 1)==0) {
					str = t2.getStrFld(2);
					bucketScan.closescan();
					break;
				}
			}
			bucketScan.closescan();
			if(str.isEmpty()) {
				str = getHashBucketInnerHeapfileName(t,hash);
				hf.insertRecord(keyTupleFromTuple(t, str).getTupleByteArray());
			}
			Tuple tmp = tupleFromRid(rid);
			new Heapfile(str).insertRecord(tmp.getTupleByteArray(), calculateMaxCapacityForKeyPair());
			current_tuples++;
            if (tuple_threshold == current_tuples) {
                Heapfile f = new Heapfile(prefix + splitPointer);
                Heapfile sf = new Heapfile(null);
                bucketScan = new Scan(f);
                Tuple t1 = null;
                RID tmpRID = new RID();
                while ((t1 = bucketScan.getNext(tmpRID)) != null) {
                    t1.setHdr((short) 2, keyFileAttr, keyFileStrLens);
                    int newBucket = calculateHashValueFromHashTuple(t1, true);
                    if (newBucket != splitPointer) {
                        sf.insertRecord(tupleFromRid(tmpRID).getTupleByteArray());
                        new Heapfile(prefix + newBucket).insertRecord(t1.getTupleByteArray());
                    }
                }
                bucketScan.closescan();
                bucketScan = new Scan(sf);
                while ((t1 = bucketScan.getNext(new RID())) != null) {
                    f.deleteRecord(ridFromTuple(t1));
                }
                sf.deleteFile();
                increamentSplit();
                setTotalTuplesAndThreshold();

            }
		}
    }
    

    private String getHashBucketInnerHeapfileName(Tuple t, int hash) throws Exception{
		String key = "";
		switch(_in[indexField-1].attrType) {
		case AttrType.attrInteger:
			key+=t.getIntFld(indexField);
			break;
		case AttrType.attrString:
			String str = t.getStrFld(indexField);
			key+=str.substring(0, Math.min(str.length(),10));
			break;
		case AttrType.attrReal:
			key+=t.getFloFld(indexField);
			break;
		}
		return key_file_prefix+hash+key;
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
    
    private int calculateHashValueFromHashTuple(Tuple t,boolean reHash) throws Exception{
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
	
	private RID ridFromTuple(Tuple t1) throws Exception {
	    try {
	      t1.setHdr((short) 2, keyPageAttr, null);
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
	    Tuple t = new Tuple();
	    try {
	      t.setHdr((short) 2, keyPageAttr, null);
	    } catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      e.printStackTrace();
	    }
	
	    int size = t.size();
	    t = new Tuple(size);
	    try {
	      t.setHdr((short) 2, keyPageAttr, null);
	    } catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      e.printStackTrace();
	    }
	    t.setIntFld(1, rid.pageNo.pid);
	    t.setIntFld(2, rid.slotNo);
	    return t;
	}
    
    private Tuple keyTupleFromTuple(Tuple t1,String name) throws Exception{
		Tuple t = new Tuple();
		t.setHdr((short)2, keyFileAttr, keyFileStrLens);
		t = new Tuple(t.size());
		t.setHdr((short)2, keyFileAttr, keyFileStrLens);
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

}
