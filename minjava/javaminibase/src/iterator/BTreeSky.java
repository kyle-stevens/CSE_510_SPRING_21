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
    private OBuf oBuf;
    //Sprojection Object declaration
    private FldSpec[] Sprojection;
    //Iterator for tuples
    private Iterator[] iter;
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
            heap = new Heapfile(file_name);
            //Initialize Buffer
            buffer = new byte[n_pages][];
            bufferPIDs = new PageId[n_pages];
            get_buffer_pages(n_pages, bufferPIDs,buffer);
            //Create new OBuf object for a computation buffer
            oBuf = new OBuf();
            Tuple w = new Tuple();
            w.setHdr(this._len_in1, this._in_1, this._t1_str_sizes);
            //Initialize OBuf object to include heapfile storage on flushed
            oBuf.init(buffer, n_pages, w.size(), heap, false);
            //Initialize FldSpec object for use in iteration
            Sprojection = new FldSpec[len_in1];
            //Initialize CondExpr array;
            cExpr = new CondExpr[pref_list_length];
            //Create iterator array
            iter = new Iterator[pref_list_length];
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
        public void runSky() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
                //Create Tuple Objects for comparisons and manipulation
                Tuple temp;
                Tuple temp2;
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
                temp = iter[0].get_next();
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
                	while((temp2=iter[i].get_next())!= null) {
                		if(TupleUtils.Equal(temp, temp2,_in_1 ,_len_in1 )) {

                        	iter[i].close();
                        	break;
                		}else {
                			oBuf.reset_read();
                			duplicate = false;
                			while((dup = oBuf.Get()) != null){
                                dup.setHdr(_len_in1, _in_1, _t1_str_sizes);
                                if(TupleUtils.Equal(dup, temp2,_in_1 ,_len_in1 )){
                                    duplicate = true;
                                    break;
                                }
                            }
                            //Need to scan HeapFile
                            if(!duplicate&&oBuf.get_buf_status()){
                                Scan scd = heap.openScan();
                                Tuple heap_dup;

                                RID heap_dup_rid = new RID();
                                //iterate over heap file.
                                while((heap_dup = scd.getNext(heap_dup_rid))!= null){
                                    heap_dup.setHdr(this._len_in1, _in_1, _t1_str_sizes);
                                    if(TupleUtils.Equal(heap_dup, temp2,_in_1 ,_len_in1 )){
                                        duplicate = true;
                                        break;
                                    }
                                }
                                scd.closescan();
                            }
                            if(!duplicate){
                                oBuf.Put(temp2);
                            }
                		}
                	}
                }
                

            //flush the buffer and store to heapfile
            oBuf.flush();

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
                	heap.deleteFile();
                }
                catch (Exception e){
                	e.printStackTrace();
                }



        }
}
