package iterator;

import java.util.Vector;
import java.util.List;
import java.io.IOException;

import bufmgr.PageNotReadException;

import global.AttrType;
import global.IndexType;
import global.PageId;
import global.GlobalConst;
import global.RID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import heap.Scan;
import heap.InvalidSlotNumberException;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
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
        //dominating Tuple
        private Tuple oneTupleToRuleThemAll;
        //Iterator for tuples
        private Iterator[] iter;
        //Heapfile for disk storage
        private Heapfile heap;
        //BlockNestedLoopSky instance
        private BlockNestedLoopSky bNLS;
        //Final Skyline
        private Vector<Tuple> skyline;
        //HeapFile Scan Object for BNLS
        private FileScan fScan;
        //CondExpr for FileScan
        private CondExpr[] cExpr;
        private String file_name;
        private AttrType _in1[];
        private short _s1_sizes[];
        private short _len_in1;
        private int _n_out_flds;
        private FldSpec[] _proj_list;
        private CondExpr[] _outFilter;
        private Iterator scan;
        private int MINIBASE_PAGESIZE = 1024;

        //Pass in Sorted Index Files Descending Order
        public BTreeSky(AttrType[] in1,
                        short len_in1,
                        short[] t1_str_sizes,
                        Iterator am1,
                        java.lang.String relationName,
                        int[] pref_list,
                        int pref_list_length,
                        String[] index_file_list,
                        int n_pages)
                        throws IndexException,
			InvalidTypeException,
                        InvalidTupleSizeException,
                        UnknownIndexTypeException,
                        IOException,
                        SortException,
                        IteratorBMException, JoinsException, IndexException, InvalidTupleSizeException,
                			 PageNotReadException, TupleUtilsException, PredEvalException, SortException,
                			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
        {
            file_name = "skyline_candidates";
            //Initialize Heap file to one named 'skyline_candidates'
            heap = new Heapfile(file_name);
            //Create new OBuf object for a computation buffer
            oBuf = new OBuf();
            //Initialize Buffer
            buffer = new byte[n_pages][];
            //Initialize OBuf object to include heapfile storage on flushed
            oBuf.init(buffer, n_pages, MINIBASE_PAGESIZE, heap, true);
            //Initialize FldSpec object for use in iteration
            Sprojection = new FldSpec[len_in1];
            //Initialize CondExpr array;
            cExpr = new CondExpr[len_in1];
            //Create iterator array
            iter = new Iterator[len_in1];
            //Iterate over IndexFiles and create separate iterators and fldspecs
            for(int j = 0; j<len_in1;j++){
		    Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
	    }
	    for(int i=0; i<len_in1;i++){
                    cExpr[i] = new CondExpr();
                    iter[i] = new IndexScan(new IndexType(IndexType.B_Index),
                    relationName, index_file_list[i], in1, t1_str_sizes,len_in1,
                    len_in1,Sprojection, null, 0, false);
            }
            _in1 = in1;
            _s1_sizes = t1_str_sizes;
            _len_in1 = len_in1;
            _n_out_flds = pref_list_length;
            _proj_list = Sprojection;
            _outFilter = cExpr;
            //Get Iterator for Heapfile
            fScan = new FileScan(file_name, _in1, _s1_sizes, _len_in1,
            _n_out_flds, _proj_list, _outFilter);
            //Initialize BNLS object to use heapfile
            bNLS = new BlockNestedLoopSky(in1, len_in1, t1_str_sizes, fScan,
            "skyline_candidates", pref_list, pref_list_length,
            n_pages);
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
                boolean common = false;
                boolean foundDominantTuple = false;
                boolean duplicate = false;
                scan = iter[0]; //setting up get_next
                //Loop over the tuples of the first index_file ( Tuple Field )
                while((temp=iter[0].get_next()) != null && !foundDominantTuple){
                    //Reset Loop Conition
                    common = false;
                    scan = iter[0]; //resetting get_next
                    //Iterate over all iterators for each index file
                    for(int i = 1; i < iter.length; i++){
                        //Reset Loop condition
                        common = false;
                        scan = iter[i]; //setting get_next
                        //So Long as no common value has been Found
                        //and there exists some next tuple in the list
                        while(((temp2=iter[i].get_next())!= null) && !common){
                            //If we have found a common tuple, end this loop iteration
                            //via the loop condition
                            if (temp == temp2){
                                common = true;
                                break;
                            }
                            //If tuples are not equal, and we have not
                            //already encountered this tuple put in the oBuf
                            //and store the tuple in a list of encountered tuples
                            //for duplicate elimination
                            else if(temp != temp2){
                                common = false;
                                while((dup = oBuf.Get()) != null){
                                    if(dup == temp2){
                                        duplicate = true;
                                        break;
                                    }
                                }
                                //Need to scan HeapFile
                                if(oBuf.get_buf_status()){
                                    Scan scd = heap.openScan();
                                    Tuple heap_dup;
                                    RID heap_dup_rid = new RID();
                                    //iterate over heap file.
                                    while((heap_dup = scd.getNext(heap_dup_rid))!= null){
                                        if(heap_dup == temp2){
                                            duplicate = true;
                                            break;
                                        }
                                    }
                                    scd.closescan();
                                }
                            }
                            if(!duplicate){
                                oBuf.Put(temp2);
                            }
                        }
                    }
                    //If the Dominant Tuple has been found, store this tuple
                    //and set loop exit condition
                    if(common){
                        oneTupleToRuleThemAll = temp;
                        foundDominantTuple = true;
                    }
                }

            //flush the buffer and store to heapfile
            oBuf.flush();
            
        }
        @Override
        public Tuple get_next() throws Exception{
                return bNLS.get_next();


        }
        @Override
        public void close() throws IOException, JoinsException, SortException, IndexException {
	// TODO Auto-generated method stub
                try{
                        heap.deleteFile();
                }
                catch (Exception e){
                        return;
                }



        }
}
