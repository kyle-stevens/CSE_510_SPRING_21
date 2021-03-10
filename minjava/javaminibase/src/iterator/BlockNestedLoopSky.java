package iterator;

import bufmgr.BufMgr;
import global.AttrType;
import global.PageId;
import global.RID;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.*;

import java.io.IOException;
import java.security.cert.TrustAnchor;
import java.util.Vector;

public class BlockNestedLoopSky extends Iterator
{
    private AttrType _in1[];
    private   int        in1_len;
    private FileScan outer;
    private   short t1_str_sizescopy[];
    private Tuple outer_tuple, inner_tuple, skyline_disk_candidate;
    private Heapfile temp_files;
    private OBuf inner;
    // rid2 - To store outer loop Tuple RID to start next iteration from. Flag values are,
    // -1 (Default) => Denotes there is no need for a second outer loop iteration since
    // the buffer has free space left and that there are no skyline candidates in the disk
    // -2 => Denotes there is need for a second outer loop iteration since
    // the Buffer is full now and that there are skyline candidates in the disk to be compared next with
    private RID rid2, cand_disk_rid_mark;
    // cand_disk_rid_mark to store the candidate skyline in disk to start
    // pushing into buffer from, for the next iteration.
    private int pref_list[];
    private int pref_list_length;
    private int n_pages;
    private boolean dominated, first_set,
            cand_disk_file,   // To indicate un-vetted skyline candidates in disk
            is_buf_full;      // To indicate once the buffer window is full in a single outer loop iteration
    PageId[] bufs_pids;
    byte[][] bufs;

    /**constructor
     *@param in1  Array containing field types of R.
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields.
     *@param am1  access method for input data
     *@param relationName  access hfapfile for input data
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     */
    public BlockNestedLoopSky( AttrType    in1[],
                           int     len_in1,
                           short   t1_str_sizes[],
                           Iterator     am1,
                           String relationName,
                           int   pref_list[],
                           int    pref_list_length,
                           int n_pages
    ) throws NestedLoopException, SortException
    {
        _in1 = new AttrType[in1.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        in1_len = len_in1;
        outer = (FileScan) am1;
        t1_str_sizescopy =  t1_str_sizes;
        outer_tuple = new Tuple();
        dominated = false;
        first_set = true;
        cand_disk_file = false;
        is_buf_full = false;
        this.pref_list = pref_list;
        this.pref_list_length = pref_list_length;
        this.n_pages = n_pages;
        rid2 = new RID();
        rid2.slotNo = -1;
        cand_disk_rid_mark = new RID();
        cand_disk_rid_mark.slotNo = -1;
        bufs_pids = new PageId[n_pages];
        bufs = new byte[n_pages][];

        try {
            //System.out.println("Getting " + n_pages + " buffer pages");
            get_buffer_pages(n_pages, bufs_pids, bufs);
        }
        catch (Exception e) {
            throw new SortException(e, "BUFmgr error");
        }

        try {
            temp_files = new Heapfile(null);
        }
        catch (Exception e) {
            throw new SortException(e, "Heapfile error");
        }

        //Buffer window instance
        inner = new OBuf();
        Tuple t = new Tuple();
        try {
            t.setHdr((short)in1_len, _in1, t1_str_sizescopy);
        }
        catch (Exception e) {
            throw new SortException(e, "t.setHdr() failed");
        }
        int tuple_size = t.size();
        inner.init(bufs, n_pages, tuple_size, temp_files, false);
    }

    public Tuple get_next(){
        return null;
    }

    /**
     *@return The skyline tuple set is returned
     *@exception NestedLoopException exception from this class
     *@exception Exception other exceptions
     */
    public Vector<Tuple> get_skyline()
            throws NestedLoopException,
            Exception
    {
        is_buf_full = false;
        RID rid1 = new RID(); // For getting RID of scanned outer loop Tuples
        RID cand_disk_rid = new RID();  // For getting RID of scanned Inner loop Tuples
        if (cand_disk_file) {
            // System.out.println("Found skyline candidates in disk: " + temp_files.getRecCnt());
            Scan scd = temp_files.openScan();
            skyline_disk_candidate = new Tuple();
            // reset pointers to start of the buffer window to start insertion from top.
            inner.reset_write();
            inner.reset_read();
            if (cand_disk_rid_mark.slotNo != -1) {
                // Position scan to last scanned tuple
                if (scd.position(cand_disk_rid_mark)){
                    System.out.println("\n");
                }
                else System.out.println("Candidate Heap Seek Fail");
                cand_disk_rid_mark.slotNo = -1;
            }
            // push un-vetted skyline candidate tuples from disk to buffer
            while ((skyline_disk_candidate = scd.getNext(cand_disk_rid)) != null)
            {
                skyline_disk_candidate.setHdr((short) in1_len, _in1, t1_str_sizescopy);
                inner.insert(skyline_disk_candidate, false);
                // Buffer full case while moving disk elements to buffer
                if (inner.get_buf_status()){
                    // System.out.println("No more buffer space. Buffer is full with candidate disk elements");
                    // System.out.println("Mark RID for next batch from disk to buffer");
                    //System.out.println(cand_disk_rid);
                    cand_disk_rid_mark.copyRid(cand_disk_rid);
                    //System.out.println(" RID marked for next batch from disk to buffer");
                    break;
                }
            }
            // System.out.println("Pushed maximum possible skyline candidates from disk to buffer at the moment");
            scd.closescan();
            if (!inner.get_buf_status()){
                // System.out.println("All candidate disk elements have been moved to buffer");
                // temp_files.deleteFile();
                cand_disk_file = false;
            }
            inner.reset_read();
        }
        // Seek to the tuple next to last vetted tuple before buffer full
        // condition, at the start of next iteration of outer loop
        if (!((rid2.slotNo == -1) || (rid2.slotNo == -2))) {
            if (outer.position(rid2)){
                System.out.println("\n");
            }
            else {
                System.out.println("Outer Loop Seek Failed");
            }
            rid2.slotNo = -1; //reset back
        }
        // scans through outer loop elements on the disk
        while ((outer_tuple = outer.get_next(rid1)) != null)
        {
            outer_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
            dominated = false;
            // System.out.println("Outer element: ===============================");
            // outer_tuple.print(_in1);
            int inner_tuple_num = 0;    // Local variable to indicate the current inner loop candidate
            // scans through skyline candidates on the buffer
            while ((inner_tuple = inner.Get()) != null)
            {
                inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
                // System.out.println("Inner element:");
                // inner_tuple.print(_in1);
                if (TupleUtils.Equal(outer_tuple, inner_tuple, _in1, in1_len)){
                    //System.out.println("Duplicate skyline candidate tuple");
                    inner_tuple_num++;
                    dominated = false;
                    break;
                }
                else if (TupleUtils.Dominates(outer_tuple, _in1, inner_tuple, _in1,
                        Short.parseShort(in1_len + ""), t1_str_sizescopy,
                        pref_list, pref_list_length)) {
                    // System.out.println("*******************Outer " +
                    //         "element dominates Inner element*********************");
                    dominated = true;
                    // System.out.println("Deleting below tuple from buffer");
                    // inner_tuple.print(_in1);
                    inner.Delete(inner_tuple_num);
                }
                else if (TupleUtils.Dominates(inner_tuple, _in1, outer_tuple, _in1,
                        Short.parseShort(in1_len + ""), t1_str_sizescopy,
                        pref_list, pref_list_length)) {
                    // System.out.println("*******************Inner " +
                    //         "element dominates Outer element*********************");
                    inner_tuple_num++;
                    dominated = false;
                    break;
                }
                else {
                    // System.out.println("=================Outer " +
                    //         "element Comparison continued====================");
                    inner_tuple_num++;
                    dominated = true;
                }
            }
            if (!is_buf_full) {
                // set to true once the buffer is full. Even if buffer gets free
                // space after this point, new skyline candidates will be
                // inserted only to heap file and not buffer window, for next batch of comparisons
                is_buf_full = inner.get_buf_status();
            }
            if ((is_buf_full) && (rid2.slotNo==-2)) {
                // Marks and stores the outer loop element rid in buffer-full case to
                // start the next iteration of outer loop from
                cand_disk_file = true;
                rid2.copyRid(rid1);
//                System.out.println("=====================RID2: Page no - " + rid2.pageNo + ", Slot No - " + rid2.slotNo);
            }
            inner.reset_read();
            if ((dominated) || (inner_tuple_num == 0)) {
                // Insert the tuple into skyline candidate list
                // if either it is the first incoming tuple or if
                // it dominates all the available candidates
                // System.out.println("***************Insert below tuple into skyline*************");
                // outer_tuple.print(_in1);
                if (is_buf_full) {
                    // insert to heap
                    inner.insert(outer_tuple, true);
                }
                else {
                    // insert to buffer window
                    inner.insert(outer_tuple, false);
                }
                if ((is_buf_full) && (rid2.slotNo==-1)) {
                    // Marks when first skyline attribute is stored in heapfile
                    rid2.slotNo=-2;
                }
            }
        }
            inner.reset_read();
            //print_skyline(inner);
            return store_skyline(inner);
    }

    /**
     * Used to print all the tuples (skyline) in the buffer
     * @param inner OBuf object of the buffer to print from
     * @throws Exception
     */
    public void print_skyline(OBuf inner) throws Exception {

        System.out.println("***********Skyline attributes***********");
        while ((inner_tuple = inner.Get()) != null) {
            inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
            inner_tuple.print(_in1);
        }
        inner.reset_read();
    }

    /**
     * Used to read and store all the tuples (skyline) in the buffer
     * @param inner OBuf object of the buffer to print from
     * @param skyline Vector to store current batch of skylines into
     * @throws Exception
     */
    public Vector<Tuple> store_skyline(OBuf inner) throws Exception {

        Vector<Tuple> skyline = new Vector<Tuple>();
        while ((inner_tuple = inner.Get()) != null) {
            inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
            skyline.add(inner_tuple); // Add skyline tuples to skyline vector
        }
        inner.reset_read();
        inner.reset_write();
        if (skyline.size() == 0){
            return null;
        }
        return skyline;
    }


    /**
     * implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error
     */
    public void close() throws IOException, IndexException, SortException {
        if (!closeFlag) {

            try {
                outer.close();
            }catch (Exception e) {
                throw new IOException("BlockNestedLoopSky.java: error in closing iterator and Scan", e);
            }

            //        System.out.println("Freeing buffer pages");
            try {
                free_buffer_pages(n_pages, bufs_pids);
            }
            catch (Exception e) {
                throw new SortException(e, "BUFmgr error");
            }
            try {
                //System.out.println("Deleting temporary heap file");
                temp_files.deleteFile();
            }
            catch (Exception e) {
                throw new SortException(e, "Heapfile error");
            }
            closeFlag = true;
        }
    }
}
