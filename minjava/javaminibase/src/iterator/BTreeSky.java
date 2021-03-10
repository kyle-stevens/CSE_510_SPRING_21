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



        private int number_of_run = 0;
        //Page Id Buffer
        private PageID[] bufPageIds;
        //Operations Buffer
        private byte[] buffer;
        //SkyLine buffer
        private OBuf oBuf;
        //Sprojection Object declaration
        private FldSpec[] Sprojection;

        //dominating Tuple
        private Tuple oneTupleToRuleThemAll;



        //tuples encountered temp list
        private ArrayList<Tuple> tuplesEncountered;

        //Iterator for tuples
        private Iterator[] iter;
        private Heapfile heap;

        //Obuf creation
        private OBufSortSky oBuf;

        //BlockNestedLoopSky
        privat BlockNestedLoopSky bNLS;

        private Vector<Tuple> skyline;
        //Pass in Sorted Index Files Descending Order
        public BTreeSky(AttrType[] in1,
                        int len_in1,
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
                        IteratorBMException
        {
                //create index file to pass to oBuf that consists of encountered tuples
                //and then add tuples to things as we go after the first dominating tuple
                heap = new Heapfile("skyline_candidates");
                oBuf = new OBuf();
                buffer = new byte[n_pages][];
                oBuf.init(buffer, n_pages, MINIBASE_PAGESIZE, heap, true);
                Sprojection = new FldSpec[len_in1];
                iter = new Iterator[len_in1];
                bNLS = new BlockNestedLoopSky(in1, len_in1, t1_str_sizes, am1, relationName, pref_list, pref_list_length, n_pages);

                for(int i=0; i<len_in1;i++){
                        Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
                        iter[i] = new IndexScan(new IndexType(IndexType.B_Index), relationName, index_file_list[i], in1[i], t1_str_sizes,len_in1,len_in1,Sprojection, null, 0, false);
                }


        }

        public Vector<Tuple> runSky() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
                                //Tuple t;
                                Tuple temp;
                                Tuple temp2;
                                boolean common = false;
                                //finds first common element, then will perform BlockNestedLoopSky on encountered tuples
                                while((temp=iter[0].get_next()) != null){
                                        common = false;
                                        //temp = t;

                                        for(int i = 1; i < iter.length; i++){
                                                common = false;
                                                while(((temp2=iter[i].get_next())!= null) && !common){
                                                        if (temp == temp2){
                                                                common = true;
                                                                //probably going to put iter.put(temp) here
                                                        }
                                                        else if(temp != temp2 && !tuplesEncountered.contains(temp2)){
                                                                common = false;
                                                                tuplesEncountered.add(temp2);
                                                                oBuf.Put(temp2);
                                                        }
                                                }
                                                if(common){
                                                        oneTupleToRuleThemAll = temp;
                                                        //oBuf.Put(oneTupleToRuleThemAll);
                                                        break; //I know its bad, but I havent thought of a better way yet
                                                }
                                        }
                                }
                                oBuf.flush();
                                skyline = bNLS.get_skyline();
                                skyline.add(0, oneTupleToRuleThemAll); //add dominant tuple to beginning of vector.
                                return skyline;
                                //calling BlockNestedLoopSky on data.




                        }

        @Override
	    public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		iter.close();
		try {
			new Heapfile(oBuf.getCurr_file() + (number_of_run - 1)).deleteFile();
		} catch (Exception e) {

		}
	}


}
