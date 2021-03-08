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
        private AttrType[] _in1;
        private int _len_in1;
        private short[] _t1_str_sizes;
        private Iterator _am1;
        private java.lang.String _relationName;
        private int[] _pref_list;
        private int[] _pref_list_length;
        private IndexFile[] _index_file_list;
        private int _n_pages;

        //Page Id Buffer
        private PageID[] bufPageIds;
        //Operations Buffer
        private byte[] buffer;
        //SkyLine buffer
        private OBufSortSky opBuf;
        //Sprojection Object declaration
        private FldSpec[] Sprojection;

        //dominating Tuple
        private Tuple oneTupleToRuleThemAll;

        //tuples encountered
        private Tuple[] tuplesEncountered;

        //tuples encountered temp list
        private ArrayList<Tuple> tuplesTempEncountered;

        //Iterator for tuples
        private Iterator[] iter;

        //Pass in Sorted Index Files Descending Order
        public BTreeSky(AttrType[] in1,
                        int len_in1,
                        short[] t1_str_sizes,
                        Iterator am1,
                        java.lang.String relationName,
                        int[] pref_list,
                        int[] pref_list_length,
                        String[] index_file_list,
                        int n_pages)
                        throws IndexException,
			InvalidTypeException,
                        InvalidTupleSizeException,
                        UnknownIndexTypeException,
                        IOException,
                        SortException,
                        IteratorBMException
        {
                //create index file to pass to oBuf that consists of encountered tuples
                //and then add tuples to things as we go after the first dominating tuple

                Tuple t;
                Sprojection = new FldSpec[len_in1];
                iter = new Iterator[len_in1];
                for(int i=0; i<len_in1;i++){
                        Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
                        iter[i] = new IndexScan(new IndexType(IndexType.B_Index), relationName, index_file_list[i], in1[i], t1_str_sizes,len_in1,len_in1,Sprojection, null, 0, false);
                }

                //will get put into @Overrid for get_next():
                Tuple temp = t;
                Tuple temp2;
                boolean common = false;
                while((t=iter[0].get_next()) != null){
                        common = false;
                        temp = t;

                        for(Iterator index_file : index_file_list){
                                common = false;
                                while(((temp2=index_file.get_next())!= null) && !common){
                                        if (temp == temp2){
                                                common = true;
                                        }
                                        else{
                                                common = false;
                                                tuplesTempEncountered.add(temp2);
                                        }
                                }
                                if(common){
                                        oneTupleToRuleThemAll = temp;
                                        break; //I know its bad, but I havent thought of a better way yet
                                }
                        }
                }
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
