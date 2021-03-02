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
    private Iterator outer1;
    private Scan outer;
    private   short t1_str_sizescopy[];
    private int amt_of_mem;
    private   int        n_buf_pgs;        // # of buffer pages available.
    private   boolean        done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple, skyline_disk_candidate;
    private Heapfile hf, temp_files;
    private BufMgr inner;
    private int pref_list[];
    private int pref_list_length;
    private int n_pages;
    private boolean dominated, first_set;
    private FileScan  outer_loop_scan;
    private Vector<Tuple> skyline;

    {
        skyline = new Vector<Tuple>();
    }

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
    ) throws NestedLoopException
    {
        _in1 = new AttrType[in1.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        in1_len = len_in1;
        outer1 = am1;
        t1_str_sizescopy =  t1_str_sizes;
        outer_tuple = new Tuple();
        String replacement_policy = new String("Clock");
        inner = new BufMgr(n_pages, replacement_policy);
        done  = false;
        get_from_outer = true;
        dominated = false;
        first_set = true;
        this.pref_list = pref_list;
        this.pref_list_length = pref_list_length;
        this.n_pages = n_pages;

        try {
            hf = new Heapfile(relationName);
            outer = hf.openScan();
        }
        catch(Exception e) {
            throw new NestedLoopException(e, "Create new heapfile or Heapfile scan failed.");
        }

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
//        System.out.println("Starting get_skyline method");
        Tuple t = new Tuple(); // need Tuple.java
        RID rid1 = new RID(); // rid1 - for getting RID of scanned outer loop (disk) Tuples
        // rid2 - To store outer loop Tuple RID to start next iteration from. Flag values are,
        // -1 (Default) => Denotes there is no need for a second outer loop iteration since
        // the buffer has free space left and that there are no skyline candidates in the disk
        // -2 => Denotes there might be a need for a second outer loop iteration since
        // the Buffer is full now and that there is a possibility of skyline candidates in the disk
        // -3 => Denotes there is need for a second outer loop iteration since
        // the Buffer is full now and that there are skyline candidates in the disk to be compared next with
        RID rid2 = new RID();
        try {
            t.setHdr((short)in1_len, _in1, t1_str_sizescopy);
        }
        catch (Exception e) {
            throw new SortException(e, "t.setHdr() failed");
        }
        int tuple_size = t.size();

        dominated = false;
        PageId[] bufs_pids = new PageId[n_pages];
        byte[][] bufs = new byte[n_pages][];

        try {
//            System.out.println("Getting " + n_pages + " buffer pages");
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
//        System.out.println("Creating OBuf object");

        OBuf inner = new OBuf();
        inner.init(bufs, n_pages, tuple_size, temp_files, false);
        RID cand_disk_rid = new RID();
        RID cand_disk_rid_mark = new RID();
        rid2.slotNo = -1;
        cand_disk_rid_mark.slotNo = -1;
        boolean cand_disk_file = false;
//        System.out.println("Starting Loops");
        do {
            // runs for number of scans on the outer loop
//            System.out.println("Check for skyline candidates in disk");
            if (cand_disk_file) {
//                System.out.println("Found skyline candidates in disk: " + temp_files.getRecCnt());
                Scan scd = temp_files.openScan();
                skyline_disk_candidate = new Tuple();
                // reset pointers to start of the buffer window to start insertion from top.
                inner.reset_write();
                inner.reset_read();
                if (cand_disk_rid_mark.slotNo != -1) {
//                    System.out.println("Positioning scan to last vetted " +
//                            "candidate disk element: " + cand_disk_rid_mark.pageNo + "-" + cand_disk_rid_mark.slotNo);

                    scd.position(cand_disk_rid_mark);
                    cand_disk_rid_mark.slotNo = -1;
//                    System.out.println(cand_disk_rid_mark.pageNo.pid);
//                    System.out.println(cand_disk_rid_mark.slotNo);
                }
//                System.out.println("Pushing skyline candidates from disk to buffer");
                // push un-vetted skyline candidate tuples from disk to buffer
                while ((skyline_disk_candidate = scd.getNext(cand_disk_rid)) != null)
                {
                    skyline_disk_candidate.setHdr((short) in1_len, _in1, t1_str_sizescopy);
//                    System.out.println("Pushing below tuple to buffer. PageNo : "+cand_disk_rid.pageNo.pid);
//                    skyline_disk_candidate.print(_in1);
                    inner.insert(skyline_disk_candidate);
                    // Buffer full case while moving disk elements to buffer
                    if (inner.get_buf_status()){
//                        System.out.println("No more buffer space. Buffer is full with candidate disk elements");
//                        System.out.println("Mark RID for next batch from disk to buffer");
                        //System.out.println(cand_disk_rid);
                        cand_disk_rid_mark.copyRid(cand_disk_rid);
                        //System.out.println(" RID marked for next batch from disk to buffer");
                        break;
                    }
                }
//                System.out.println("Pushed maximum possible skyline candidates from disk to buffer at the moment");
                scd.closescan();
                if (!inner.get_buf_status()){
//                    System.out.println("All candidate disk elements have been moved to buffer");
//                    temp_files.deleteFile();
                    cand_disk_file = false;
                }
            }
            // Seek to the tuple next to last vetted tuple before buffer full
            // condition, at the start of next iteration of outer loop
            if (!((rid2.slotNo == -1) || (rid2.slotNo == -2) || (rid2.slotNo == -3))) {
                outer = hf.openScan();
//                System.out.println("Opening a new scan");
//                System.out.println("Positioning scan next to last vetted " +
//                        "outer loop element: " + rid2.pageNo + "-" + rid2.slotNo);
                if (outer.position(rid2)){
                    System.out.println("\n");
//                    System.out.println("Seek successful");
                }
                else {
                    System.out.println("Seek Failed");
                }
                rid2.slotNo = -1; //reset back
            }
//            System.out.println("Starting outer loop elements scan ");
            while ((outer_tuple = outer.getNext(rid1)) != null)
            {
                outer_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
//                System.out.println("Outer loop element:");
//                outer_tuple.print(_in1);
                // scans through outer loop elements on the disk
                dominated = false;
                int inner_tuple_num = 0;    // Local variable to indicate the current inner loop candidate
                while ((inner_tuple = inner.Get()) != null)
                {
                    // scans through skyline candidates on the buffer
                    inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
//                    System.out.println("Inner loop element:");
//                    inner_tuple.print(_in1);
                    if (TupleUtils.Equal(outer_tuple, inner_tuple, _in1, in1_len)){
//                        System.out.println("Duplicate skyline candidate tuple");
                        inner_tuple_num++;
                        dominated = false;
                        break;
                    }
                    else if (TupleUtils.Equal_pref(outer_tuple, _in1, inner_tuple, _in1,
                            Short.parseShort(in1_len + ""), t1_str_sizescopy,
                            pref_list, pref_list_length)){
//                        System.out.println("=================Outer " +
//                                "element Equals Inner element====================");
                        inner_tuple_num++;
                        dominated = true;
                    }
                    else if (TupleUtils.Dominates(outer_tuple, _in1, inner_tuple, _in1,
                            Short.parseShort(in1_len + ""), t1_str_sizescopy,
                            pref_list, pref_list_length)) {
//                        System.out.println("*******************Outer " +
//                                "element dominates Inner element*********************");
                        dominated = true;
//                        System.out.println("Deleting below tuple from buffer");
//                        inner_tuple.print(_in1);
                        // Modify Delete() implementation with Bitmap concept
                        inner.Delete(inner_tuple_num);
                    }
                    else {
//                        System.out.println("No domination");
                        inner_tuple_num++;
                        dominated = false;
                        break;
                    }
                }
                if ((inner.get_buf_status()) && (rid2.slotNo==-3)) {
                    // Marks and stores the outer loop element rid in buffer-full case to
                    // start the next iteration of outer loop from
                    cand_disk_file = true;
//                    System.out.println("Storing RID " + rid1.pageNo + "-" + rid1.slotNo);
                    rid2.copyRid(rid1);
//                        System.out.println("Stored to RID2 " + rid2.pageNo + "-" + rid2.slotNo);
                }
                inner.reset_read();
                if ((dominated) || (inner_tuple_num == 0)) {
                    // Insert the tuple into skyline candidate list
                    // if either it is the first incoming tuple or if
                    // it dominates all the available candidates
//                    System.out.println("***************Insert below tuple into skyline*************");
//                    outer_tuple.print(_in1);
                    inner.insert(outer_tuple);
                    if ((inner.get_buf_status()) && (rid2.slotNo==-2)) {
                        // Marks when first skyline attribute is stored in heapfile
                        rid2.slotNo=-3;
                    }
                    if ((inner.get_buf_status()) && (rid2.slotNo==-1)) {
                        // Marks when buffer is full after inserting last
                        // skyline candidate in current outer loop scan
//                        System.out.println("Buffer is full. " +
//                                "Any New skyline candidates will be added to a new " +
//                                "heapfile in disk to be vetted later");
                        rid2.slotNo=-2;
                    }
                }
            }
            inner.reset_read();
            outer.closescan();
//            print_skyline(inner);
            store_skyline(inner);
            //repeat when there are unvetted elements on the disk
        }while(cand_disk_file);
        // Implement tuple set return part rather than printing
//        System.out.println("Freeing buffer pages");
        try {
            free_buffer_pages(n_pages, bufs_pids);
        }
        catch (Exception e) {
            throw new SortException(e, "BUFmgr error");
        }
        try {
//            System.out.println("Deleting temporary heap file");
            temp_files.deleteFile();
        }
        catch (Exception e) {
            throw new SortException(e, "Heapfile error");
        }
        return skyline;
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
     * @throws Exception
     */
    public void store_skyline(OBuf inner) throws Exception {

        while ((inner_tuple = inner.Get()) != null) {
            inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizescopy);
            skyline.add(inner_tuple); // Add skyline tuples to skyline vector
        }
        inner.reset_read();
    }


    /**
     * implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error
     */
    public void close() throws IOException, IndexException {
        if (!closeFlag) {

            try {
                outer1.close();
            }catch (Exception e) {
                throw new IOException("NestLoopSky.java: error in closing iterator.", e);
            }
            closeFlag = true;
        }
    }
}
