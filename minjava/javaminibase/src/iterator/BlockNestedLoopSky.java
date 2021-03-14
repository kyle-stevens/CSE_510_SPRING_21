package iterator;

import java.io.IOException;

import global.AttrType;
import global.PageId;
import global.RID;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;

public class BlockNestedLoopSky extends Iterator
{
    private AttrType _in1[];
    private   int in1_len;
    private Scan outer;
    private   short t1_str_sizescopy[];
    private Tuple outer_tuple;
    private Heapfile temp_files;
    private BlockNestedBuf inner;
    
    private int pref_list[];
    private int pref_list_length;
    private int n_pages;
    private String relationName;
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
            this.n_pages = n_pages - 8;
        }

        _in1 = new AttrType[in1.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        in1_len = len_in1;
        outer = new Scan(new Heapfile(relationName));     //Uses 2 buffer pages
        t1_str_sizescopy =  t1_str_sizes;
        this.pref_list = pref_list;
        this.pref_list_length = pref_list_length;

        this.relationName = relationName;
        bufs_pids = new PageId[this.n_pages];
        bufs = new byte[this.n_pages][];

        try {
            temp_files = new Heapfile("BlockNestedLoop");    // Uses 2 pages, but unpinned immediately after creation
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

        inner = new BlockNestedBuf(_in1,(short)in1_len,t1_str_sizescopy,bufs,pref_list,pref_list_length,this.n_pages,temp_files);
       
        
    }
    public Tuple get_next() throws Exception{
    	Tuple t = null;
    	RID rid = new RID();
    	while((t=outer.getNext(rid))!=null) {
    		t.setHdr((short)in1_len, _in1, t1_str_sizescopy);
    		inner.checkIfDominates(t);
    		
    	}
    	t = inner.getSkyTuple();
    	if(t==null) {
    		if(inner.isFlag()) {
    			outer.closescan();
    			outer = new Scan(new Heapfile(inner.getCurr_file() + inner.getNumber_of_window_file()));
    			if (inner.getNumber_of_window_file() > 0) {
    				new Heapfile(inner.getCurr_file() + (inner.getNumber_of_window_file() - 1)).deleteFile();
    			}
    			inner.setNumber_of_window_file(inner.getNumber_of_window_file()+1);
    			inner.setFlag(false);
    			inner.init();
    			return get_next();
    		}
    	}
        return t;
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
                outer.closescan();
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