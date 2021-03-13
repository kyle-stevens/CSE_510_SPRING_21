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
    private int n_pages, call_cnt;
    private boolean dominated, first_set,
            cand_disk_file,   // To indicate un-vetted skyline candidates in disk
            is_buf_full,    // To indicate once the buffer window is full in a single outer loop iteration
            is_duplicate;
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
    ) throws NestedLoopException, SortException, Exception
    {
        if (n_pages <= 5){
            throw new Exception("Not enough Buffer pages. " +
                    "\n Minimum required: 6 \t Available: " + n_pages);
        }
        else {
            this.n_pages = n_pages - 5;
        }

        _in1 = new AttrType[in1.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        in1_len = len_in1;
        outer = (FileScan) am1;     //Uses 2 buffer pages
        t1_str_sizescopy =  t1_str_sizes;
        outer_tuple = new Tuple();
        dominated = false;
        first_set = true;
        cand_disk_file = false;
        is_buf_full = false;
        is_duplicate = false;
        this.pref_list = pref_list;
        this.pref_list_length = pref_list_length;

        rid2 = new RID();
        rid2.slotNo = -1;
        rid2.pageNo.pid = -1;
        cand_disk_rid_mark = new RID();
        cand_disk_rid_mark.slotNo = -1;
        bufs_pids = new PageId[this.n_pages];
        bufs = new byte[this.n_pages][];

        call_cnt = 0;

        try {
            temp_files = new Heapfile(null);    // Uses 2 pages, but unpinned immediately after creation
            // 2 (scan) + 1 (writing) = 3 pages are reserved for later use

        }
        catch (Exception e) {
            throw new SortException(e, "Heapfile error");
        }

        try {
            get_buffer_pages(this.n_pages, bufs_pids, bufs);    // get this.n_pages buffer pages
        }
        catch (Exception e) {
            throw new SortException(e, "BUFmgr error");
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
        inner.init(bufs, this.n_pages, tuple_size, temp_files, false);
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
        call_cnt++;
//        System.out.println("\nGet_Skyline() Iteration - " + call_cnt + "\n");
        is_buf_full = false;
        RID rid1 = new RID(); // For getting RID of scanned outer loop Tuples
        RID cand_disk_rid = new RID();  // For getting RID of scanned Inner loop Tuples
        int inner_tuple_num = 0;
        int t_per_pg = inner.get_t_per_page();
        int first = 0, last = 0;

        if (cand_disk_file) {

//            System.out.println("\nFound Candidates on Disk\n");

            // Found skyline candidates on temp heap file (disk)
            Scan scd = temp_files.openScan();
            skyline_disk_candidate = new Tuple();
            // reset pointers to start of the buffer window to start insertion from top.
            inner.reset_write();
            inner.reset_read();


            if (!(cand_disk_rid_mark.slotNo == -1)) {
                // Position scan to last scanned tuple
                if (scd.position(cand_disk_rid_mark)){
//                    System.out.println("\nSeeked to Cand_RID PageNo: " + cand_disk_rid_mark.pageNo
//                            + ", Slot No: " + cand_disk_rid_mark.slotNo);
//                    System.out.println("Tuple No - " + (cand_disk_rid_mark.pageNo.pid*t_per_pg + cand_disk_rid_mark.slotNo) + "\n");
//                    System.out.println("\n");
                }
                else System.out.println("Candidate Heap Seek Fail");
                cand_disk_rid_mark.slotNo = -1;
            }
            int cand_tuple_on_heap_num = 0;

            // push un-vetted skyline candidate tuples from disk to buffer
            while ((skyline_disk_candidate = scd.getNext(cand_disk_rid)) != null) {
                skyline_disk_candidate.setHdr((short) in1_len, _in1, t1_str_sizescopy);
                dominated = false;
                if (cand_tuple_on_heap_num == 0) {
                    first = ((t_per_pg*cand_disk_rid.pageNo.pid) + cand_disk_rid.slotNo);
//                    System.out.println("\nPushing " + temp_files.getRecCnt() + " Disk to Buffer starting from");
//                    skyline_disk_candidate.print(_in1);
//                    System.out.println("Cand_RID - PageNo: " + cand_disk_rid.pageNo + ", SlotNo: " + cand_disk_rid.slotNo + "\n");
                }
                cand_tuple_on_heap_num++;
                while ((inner_tuple = inner.Get()) != null) {
                    inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);

//                    System.out.println("D-to-B : Inner Loop element");
//                    inner_tuple.print(_in1);

                    if (TupleUtils.Equal(skyline_disk_candidate, inner_tuple, _in1, in1_len)) {
                        // Duplicate tuple
//                        System.out.println("D-to-B : Duplicate Tuple");
                        inner_tuple_num++;
                        is_duplicate = true;
                        dominated = false;
                    } else if (TupleUtils.Dominates(skyline_disk_candidate, _in1, inner_tuple, _in1,
                            Short.parseShort(in1_len + ""), t1_str_sizescopy,
                            pref_list, pref_list_length)) {
                        // Outer tuple dominates inner tuple
                        dominated = true;
//                        System.out.println("D-to-B : Deleting %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
//                        inner_tuple.print(_in1);
                        inner.Delete(inner_tuple_num);
                    } else if (TupleUtils.Dominates(inner_tuple, _in1, skyline_disk_candidate, _in1,
                            Short.parseShort(in1_len + ""), t1_str_sizescopy,
                            pref_list, pref_list_length)) {
                        // Inner tuple dominates Outer tuple
                        inner_tuple_num++;
                        dominated = false;
                        break;
                    } else {
                        // neither of them dominate each other
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

                if (dominated || (inner_tuple_num == 0)) {
                    if (is_duplicate) {
                        is_duplicate = false;
                    }
                    else if (!is_buf_full) {
                        inner.insert(skyline_disk_candidate);
                    }
                    else if (cand_disk_rid_mark.slotNo == -1) {
                        // Buffer full case while moving disk elements to buffer
//                        System.out.println("\nMarking Temp file RID");
////                        skyline_disk_candidate.print(_in1);
//                        System.out.println("Cand_RID_Mark - PageNo: " + cand_disk_rid.pageNo + ", SlotNo: " + cand_disk_rid.slotNo + "\n");
//                        System.out.println("Tuple No - " + (cand_disk_rid.pageNo.pid*t_per_pg + cand_disk_rid.slotNo) + "\n");
//                cand_disk_file = true;
                        cand_disk_rid_mark.pageNo.pid = cand_disk_rid.pageNo.pid;
                        cand_disk_rid_mark.slotNo = cand_disk_rid.slotNo;
                        break;
                    }
                }
                else {
//                    System.out.println("D-to-B : Dropping tuple from Temp heap file");
                }
            }
            last = ((t_per_pg*cand_disk_rid.pageNo.pid) + cand_disk_rid.slotNo);
//            System.out.println("\nPushed max possible " + cand_tuple_on_heap_num + " candidates from disk to buffer");
//            System.out.println("TupleNo: From " + first + " to " + last + "\n");
            scd.closescan();

            if (!inner.get_buf_status()){
                // temp heap file (disk) is empty
//                System.out.println("\nAll Disk elements pushed successfully\n");
                cand_disk_file = false;

            }

            inner.reset_read();
        }
        is_buf_full = false;
        is_duplicate = false;
        dominated = false;
        first = last = -1;
        int heap_first = -1, heap_last = -1, heap_cnt = 0;

        // Seek to the tuple next to last vetted tuple before buffer full
        // condition, at the start of next iteration of outer loop
        if (!((rid2.slotNo == -1) || (rid2.slotNo == -2))) {
            if (outer.position(rid2)){
                System.out.println("\n");
            }
            else {
                System.out.println("Outer Loop Seek Failed");
            }
            rid2.pageNo.pid = -1;
            rid2.slotNo = -1; //reset back
        }

        int outer_tuple_num = 0;    // Local variable to indicate the current Outer loop candidate
        // scans through outer loop elements on the disk
        while ((outer_tuple = outer.get_next(rid1)) != null)
        {
            outer_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
            dominated = false;
            inner_tuple_num = 0;    // Local variable to indicate the current inner loop candidate

            if (outer_tuple_num==0){
                first = ((t_per_pg*rid1.pageNo.pid) + rid1.slotNo);
//                System.out.println("\nOuter loop starting element");
//                outer_tuple.print(_in1);
//                System.out.println("RID - PageNo: " + rid1.pageNo + ", SlotNo: " + rid1.slotNo + "\n");
            }
            outer_tuple_num++;
//            System.out.println("\nOuter Loop Iteration - " + outer_tuple_num + "\n");
//            System.out.println("Outer Loop element============================================");
//            outer_tuple.print(_in1);

            // scans through skyline candidates on the buffer
            while ((inner_tuple = inner.Get()) != null)
            {
                inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);

//                System.out.println("Inner Loop element");
//                inner_tuple.print(_in1);

                if (TupleUtils.Equal(outer_tuple, inner_tuple, _in1, in1_len)){
                    // Duplicate tuple
//                    System.out.println("Duplicate Tuple");
                    inner_tuple_num++;
                    dominated = false;
                    is_duplicate=true;
//                    break;
                }

                else if (TupleUtils.Dominates(outer_tuple, _in1, inner_tuple, _in1,
                        Short.parseShort(in1_len + ""), t1_str_sizescopy,
                        pref_list, pref_list_length)) {
                    // Outer tuple dominates inner tuple
                    dominated = true;
//                    System.out.println("Deleting%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
//                    inner_tuple.print(_in1);
                    inner.Delete(inner_tuple_num);
                }

                else if (TupleUtils.Dominates(inner_tuple, _in1, outer_tuple, _in1,
                        Short.parseShort(in1_len + ""), t1_str_sizescopy,
                        pref_list, pref_list_length)) {
                    // Inner tuple dominates Outer tuple
                    inner_tuple_num++;
                    dominated = false;
                    break;
                }

                else {
                    // neither of them dominate each other
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
//                System.out.println("\nMarking outer loop RID");
//                outer_tuple.print(_in1);
//                System.out.println("RID2 - PageNo: " + rid1.pageNo + ", SlotNo: " + rid1.slotNo + "\n");
                cand_disk_file = true;
                rid2.pageNo.pid = rid1.pageNo.pid;
                rid2.slotNo = rid1.slotNo;
            }

            inner.reset_read();

            if ((dominated) || (inner_tuple_num == 0)) {
                // Insert the tuple into skyline candidate list
                // if either it is the first incoming tuple or if
                // it dominates all the available candidates
                if (is_duplicate){
                    is_duplicate = false;
                    continue;
                }
                else if (is_buf_full) {
                    // insert to heap
//                    System.out.println("Inserting into HEAP ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//                    outer_tuple.print(_in1);
                    if (heap_first == -1) {
                        heap_first = ((t_per_pg*rid1.pageNo.pid) + rid1.slotNo);
                    }
                    if (cand_disk_rid_mark.slotNo == -1) {
                        cand_disk_rid_mark = inner.insert_heap(outer_tuple, true);    // Uses 1 buffer page
//                        System.out.println("INSERT MARKING Cand_RID - PageNo : " +
//                                cand_disk_rid_mark.pageNo + ", SlotNo: " + cand_disk_rid_mark.slotNo);
//                        System.out.println("Tuple No - " + (cand_disk_rid_mark.pageNo.pid*t_per_pg + cand_disk_rid_mark.slotNo) + "\n");
                    }
                    else {
                        inner.insert_heap(outer_tuple, true);
                    }
                    heap_cnt++;
                }
                else {
                    // insert to buffer window
//                    System.out.println("Inserting into Buffer &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
//                    outer_tuple.print(_in1);
                    inner.insert(outer_tuple);
                }
                if ((is_buf_full) && (rid2.slotNo==-1)) {
                    // Marks when first skyline attribute is stored in heapfile
//                    System.out.println("Marking Entry into Heapfile");
//                    outer_tuple.print(_in1);
//                    System.out.println("RID1 - PageNo: " + rid1.pageNo + ", SlotNo: " + rid1.slotNo + "\n");
                    rid2.slotNo=-2;
                }
            }
        }
        last = ((t_per_pg*rid1.pageNo.pid) + rid1.slotNo);
//        System.out.println("\nVetted below range of outer tuples with buffer");
//        System.out.println("TupleNo: From " + first + " to " + last + "\n");
//        if (heap_first != -1){
//            heap_last = ((t_per_pg*rid1.pageNo.pid) + rid1.slotNo);
//            System.out.println("\nInserted " + heap_cnt + " outer tuples to temp disk file");
//            System.out.println("First tuple entered into disk in this iteration : " + heap_first);
//            System.out.println("Total records in temp heap file is : " + temp_files.getRecCnt() + "\n");
//
////            System.out.println("\nTupleNo: From " + first + " to " + last + "\n");
//        }

        if (outer_tuple_num==0) {
            // No more outer loop elements to check
            inner.reset_read();
            inner.reset_write();
            return null;
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

        if ((skyline.size() == 0) && !(cand_disk_file)){
            return null;
        }
        System.out.println("\n Number of Skylines found : " + skyline.size() + "\n");
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

            try {
                free_buffer_pages(this.n_pages, bufs_pids);
            }
            catch (Exception e) {
                throw new SortException(e, "BUFmgr error");
            }

            try {
                temp_files.deleteFile();
            }
            catch (Exception e) {
                throw new SortException(e, "Heapfile error");
            }
            closeFlag = true;
        }
    }
}
