package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;

import global.AttrType;
import global.IndexType;
import global.PageId;
import global.RID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import heap.Scan;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;


public class BTreeSky extends Iterator{



	   //Operations Buffer
    private byte[][] buffer;
    //SkyLine buffer
    private BTreeSkyBuf oBuf;
    //Sprojection Object declaration
    private FldSpec[] Sprojection;
    //Iterator for tuples
    private IndexScan[] iter;
    //Heapfile for disk storage
    private Heapfile heap;
    
    //BlockNestedLoopSky instance
    private BlockNestedLoopSky bNLS;
    //HeapFile Scan Object for BNLS
    private FileScan fScan;
    //CondExpr for FileScan
    private CondExpr[] cExpr;
    private String file_name;



    private int _n_out_flds;
    private FldSpec[] _proj_list;
    private CondExpr[] _outFilter;
    private Iterator scan;
    private PageId[] bufferPIDs;
    private int _n_Pages;
    private AttrType[] _in_1;
    private short _len_in1;
    private short[] _t1_str_sizes;
    private int[] pref_list;
    private int pref_list_length;
    private String relationName ;
    private String[] indexNames;
    
    private Heapfile blockNestedFile;
    private Heapfile _dataFile;


        //Pass in Sorted Index Files Descending Order
        public BTreeSky(AttrType[] in1,
                        short len_in1,
                        short[] t1_str_sizes,
                        Iterator am1,
                        String relationName,
                        int[] pref_list,
                        int pref_list_length,
                        String[] index_file_list,
                        int n_pages)
                        throws Exception
        {
        	n_pages-=4;
        	if(n_pages<=0) throw new Exception("Not enough pages for BTreeSky");
        	this.relationName = relationName;
            this._len_in1 = len_in1;
            this._in_1 = in1;
            this._t1_str_sizes = t1_str_sizes;
            this._n_Pages = n_pages;
            this.pref_list = pref_list;
            this.pref_list_length = pref_list_length;
            this.indexNames = index_file_list;
            file_name = "skyline_candidates";
            //Initialize Heap file to one named 'skyline_candidates'
            heap = new Heapfile(null);
            blockNestedFile = new Heapfile(file_name);
            _dataFile = new Heapfile(relationName);
            //Initialize Buffer
            buffer = new byte[n_pages][];
            bufferPIDs = new PageId[n_pages];
            get_buffer_pages(n_pages, bufferPIDs,buffer);
            //Create new OBuf object for a computation buffer
            oBuf = new BTreeSkyBuf(buffer, n_pages, heap,blockNestedFile,_dataFile);
            //Initialize FldSpec object for use in iteration
            Sprojection = new FldSpec[len_in1];
            //Initialize CondExpr array;
            cExpr = new CondExpr[pref_list_length];
            //Create iterator array
            iter = new IndexScan[pref_list_length];
            //Iterate over IndexFiles and create separate iterators and fldspecs
            for(int j = 0; j<len_in1;j++){
		    Sprojection[j] = new FldSpec(new RelSpec(RelSpec.outer), j+1);
	    }
	    for(int i=0; i<pref_list_length;i++){
                    cExpr[i] = new CondExpr();
                    
            }
            this._in_1 = in1;

            _len_in1 = len_in1;
            _n_out_flds = pref_list_length;
            _proj_list = Sprojection;
            _outFilter = cExpr;
            //Get Iterator for Heapfile
            //Initialize BNLS object to use heapfile
            runSky();

        }
        /**
        *implement the skyline operations using BlockNestedLoopSky
        *and perform tuple comparision to find skyline candidates to pass
        *into the bNLS object.
        *
        */
        public void runSky() throws Exception{
                //Create Tuple Objects for comparisons and manipulation
                RID temp;
                RID temp2;
                Tuple dup;
                //Set Loop and exit condition
                boolean duplicate = false;
                iter[0] = new IndexScan(new IndexType(IndexType.B_Index),
                        relationName,
                        indexNames[0],
                        _in_1,
                        _t1_str_sizes,
                        _len_in1,
                        _len_in1,
                        Sprojection,
                        null,
                        0,
                        false);
                scan = iter[0]; //setting up get_next
                //Loop over the tuples of the first index_file ( Tuple Field )
                temp = iter[0].getNext();
                oBuf.Put(temp);
                iter[0].close();
                for(int i = 1; i < iter.length; i++){
                	 iter[i] = new IndexScan(new IndexType(IndexType.B_Index),
                             relationName,
                             indexNames[i],
                             _in_1,
                             _t1_str_sizes,
                             _len_in1,
                             _len_in1,
                             Sprojection,
                             null,
                             0,
                             false);
                	scan = iter[i];
                	while((temp2=iter[i].getNext())!= null) {
                		if(temp.equals(temp2))
                		{
                			Tuple t2 = _dataFile.getRecord(temp2);
                			t2.setHdr(_len_in1, _in_1, _t1_str_sizes);
                			RID rid2 = null;
                			while((rid2=iter[i].getNext())!=null) {
                    			Tuple t3 = _dataFile.getRecord(rid2);
                    			t3.setHdr(_len_in1, _in_1, _t1_str_sizes);
                				if(!TupleUtils.Equal(t3, t2, _in_1, _len_in1))
                					break;
                				oBuf.Put(rid2);
                			}
                			iter[i].close();
                			break;
                		}
                		oBuf.Put(temp2);
                	}
                }
                //heap.deleteFile();
              System.out.println(blockNestedFile.getRecCnt());
    		free_buffer_pages(_n_Pages, bufferPIDs);
    		bNLS = new BlockNestedLoopSky(_in_1, _len_in1, _t1_str_sizes, fScan,
        file_name, pref_list, pref_list_length,
        _n_Pages+4);
        }
        
        

        @Override
        public Tuple get_next() throws Exception{
        	
        	
                return bNLS.get_next();


        }
        @Override
        public void close() throws IOException, JoinsException, SortException, IndexException {
	// TODO Auto-generated method stub
                try{
                	bNLS.close();
                }
                catch (Exception e){
                	e.printStackTrace();
                }



        }
}
