package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.IndexType;
import global.PageId;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;

public class BTreeSky(){
        //Input Parameters variable declarations
        private AttrType[] in1;
        private int len_in1;
        private short[] t1_str_sizes;
        private Iterator am1;
        private java.lang.String relationName;
        private int[] pref_list;
        private int[] pref_list_length;
        private String[] index_file_list;
        private int n_pages;

        //Page Id Buffer
        private PageID[] bufPageIds;
        //Operations Buffer
        private byte[] buffer;
        //SkyLine buffer
        private OBufSortSky opBuf;
        //Sprojection Object declaration
        private FldSpec[] Sprojection;

        public BTreeSky(AttrType[] in1, int len_in1, short[] t1_str_sizes,
                Iterator am1, java.lang.String relationName, int[] pref_list,
                int[] pref_list_length, String[] index_file_list, int n_pages){
                        this.in1 = in1;
                        this.len_in1 = len_in1;

                         this.in1 = in1;
                         this.len_in1 = len_in1;
                         this.t1_str_sizes = str_sizes;
                         this.am1 = am1;
                         this.relationName = relationName;
                         this.pref_list = pref_list;
                         this.pref_list_length = pref_list_length;
                         this.index_file_list = index_file_list;
                         this.n_pages = n_pages;

                         //Buffer for Page Ids Initialized to empty array
                         this.bufPageIds = new PageId[this.n_pages];

                         //Buffer created for operations
                         this.buffer = new byte[this.n_pages][];
                         //Grab Buffer Pages for used
                         get_buffer_pages(this.n_pages, this.bufPageIds,
                                this.buffer);

                         //Initialize OBufSortSky object for computations
                         this.opBuf = new OBufSortSky(this.in1, this,len_in1,
                                this.t1_str_sizes, this.buffer, this.pref_list,
                                this.pref_list_length, this.n_pages);

                         //Initialize Sprojection
                         this.Sprojection = new FldSpec[this.len_in1];


                }
}
