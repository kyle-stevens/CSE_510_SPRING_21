package iterator;

//Until better solution is found
import java.util.ArrayList;
import java.util.List;

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

public class BTreeSky extends Iterator{
        //Input Parameters variable declarations
        private AttrType[] in1;
        private int len_in1;
        private short[] t1_str_sizes;
        private Iterator am1;
        private java.lang.String relationName;
        private int[] pref_list;
        private int[] pref_list_length;
        private IndexFile[] index_file_list;
        private int n_pages;

        //Page Id Buffer
        private PageID[] bufPageIds;
        //Operations Buffer
        private byte[] buffer;
        //SkyLine buffer
        private OBufSortSky opBuf;
        //Sprojection Object declaration
        private FldSpec[] Sprojection;

        //dominating Tuple
        private Tuple tupleDominates;

        //tuples encountered
        private Tuple[] tuplesEncountered;

        //tuples encountered temp list
        private ArrayList<Tuple> tuplesTempEncountered;

        public BTreeSky(AttrType[] in1,
                        int len_in1,
                        short[] t1_str_sizes,
                        Iterator am1,
                        java.lang.String relationName,
                        int[] pref_list,
                        int[] pref_list_length,
                        IndexFile[] index_file_list,
                        int n_pages)
                        throws IndexException,
			InvalidTypeException,
                        InvalidTupleSizeException,
                        UnknownIndexTypeException,
                        IOException,
                        SortException,
                        IteratorBMException
        {
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

                 //initialize
                 this.tuplesTempEncountered = new ArrayList<Tuple>();

                 //Starting anew

                 /*

                 for tuples in index_file_list[0]:
                        temp = tuples
                        common = false
                        for lists in index_file_list[1:]:
                                common = false
                                for tuplesInner in  lists and !common:

                                        if temp = tuplesInner:
                                                common = true
                                        else:
                                                tuplesEncountered.add(tuplesInner)
                                                common = false
                        if common:
                                dominating tuple is temp
                                break
                 */
        }


}
