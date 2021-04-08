package btree;

import btree.BT;
import btree.BTreeFile;
import btree.RealKey;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import iterator.Iterator;
import iterator.Sort;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import iterator.*;


public class ClusteredBtreeIndex {

        private static BTreeFile bTreeFile;

        private static Heapfile sortedDataFile;

        public ClusteredBtreeIndex(String sortedDataFileName, String indexFileName, int keySize,
                int delete_fashion, int recSize, Iterator scan,
                AttrType attrTypes[], short[] str_sizes, int indexAttr,
                int n_pages) throws Exception {

                        bTreeFile = new BTreeFile(indexFileName, attrTypes[indexAttr].attrType, keySize, delete_fashion);

                        Sort sort = new Sort(attrTypes, (short)attrTypes.length, str_sizes, scan,
                        indexAttr, new TupleOrder(TupleOrder.Ascending), keySize, n_pages);

                        sortedDataFile = new Heapfile(sortedDataFileName,true);

                        int maxPageCapacity = (GlobalConst.MINIBASE_PAGESIZE * GlobalConst.MAX_PAGE_UTILIZATION)/(100* (recSize+4));

                        Tuple next_tuple = sort.get_next();

                        for(int tupleCount = 1; next_tuple != null; next_tuple = sort.get_next(), tupleCount++) {
                        RID keyTupleRid = sortedDataFile.insertRecord(next_tuple.returnTupleByteArray(), maxPageCapacity);
                                if (tupleCount % maxPageCapacity == 0) { //reason for this needs to be commented
                                /*bTreeFile.*/insert(new RealKey(next_tuple.getFloFld(indexAttr)), keyTupleRid);
                                }
                        }
                }

        public BTFileScan new_cluster_scan(KeyClass lo_key, KeyClass hi_key){
                try{
                        return(bTreeFile.new_scan(lo_key, hi_key));
                }catch(Exception e){
                        e.printStackTrace();
                        return null;
                }
        }

        //not sure about this func purpose
        void trace_children(PageId id){
                try{
                        bTreeFile.trace_children(id);
                }catch(Exception e){
                        e.printStackTrace();
                }
        }

        public void printCBtree() {
                try {
                        BT.printBTree(bTreeFile.getHeaderPage());
                        BT.printAllLeafPages(bTreeFile.getHeaderPage());
                }catch (Exception e) {
                        e.printStackTrace();
                }
        }

        //makes our isnertion easeier and cleaner, especially for calls outside of class
        public void insert(KeyClass key, RID rid){
                try{
                        bTreeFile.insert(key, rid);
                }catch(Exception e){
                        e.printStackTrace();
                }
        }

        public boolean Delete(KeyClass key, RID rid){
                try{
                        return(bTreeFile.Delete(key, rid));
                }catch(Exception e){
                        e.printStackTrace();
                        return false;
                }
        }

        public void close(){
                try{
                        bTreeFile.close();
                }catch(Exception e){
                        e.printStackTrace();
                }
        }

        public static void traceFilename(String filename){
                try{
                        bTreeFile.traceFilename(filename);
                }catch(Exception e){
                        e.printStackTrace();
                }
        }

        public static void destroyTrace(){
                try{
                        bTreeFile.destroyTrace();
                }catch(Exception e){
                        e.printStackTrace();
                }
        }

        public BTreeHeaderPage getHeaderPage(){
                try{
                        return (bTreeFile.getHeaderPage());
                }catch(Exception e){
                        e.printStackTrace();
                        return null;
                }
        }

        public void destroyFile(){
                try{
                        bTreeFile.destroyFile();
                }catch(Exception e){
                        e.printStackTrace();
                }
        }


}
