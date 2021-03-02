package iterator;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;

import java.io.*;

/**
 *O_buf::Put takes tuples and stores them on the buffer pages that
 *were passed to O_buf::init.  O_buf::flush inserts them enmass into
 *a temporary HeapFile.
 */
public class OBuf implements GlobalConst{
  
  /**
   *fault constructor
   * no args -- use init to initialize
   */
  public OBuf(){}
  


  /**
   * O_buf is an output buffer. It takes as input:
   *@param bufs  temporary buffer to pages.(EACH ELEMENT IS A SINGLE BUFFER PAGE).
   *@param n_pages the number of pages
   *@param tSize   tuple size
   *@param temp_fd  fd of a  HeapFile
   *@param buffer  true => it is used as a buffer => if it is flushed, print
   *                      a nasty message. it is false by default.
  */
  public void init(byte[][] bufs, int n_pages, int tSize,
		   Heapfile temp_fd, boolean buffer )
    {
      _bufs    = bufs;
      _n_pages = n_pages;
      t_size   = tSize;
      _temp_fd = temp_fd;
      
      dirty       = false;
      t_per_pg    = MINIBASE_PAGESIZE / t_size;
      t_in_buf    = n_pages * t_per_pg;
      t_wr_to_pg  = 0;
      t_wr_to_buf = 0;
      t_written   = 0L;
      t_rd_from_pg  = 0;
      t_rd_from_buf = 0;
      t_read   = 0L;
      curr_page   = 0;
      rd_curr_page = 0;
//      total_pg_used = 0;
//      total_tuples_present = 0;
      buffer_only = buffer;
      is_buf_full = false;
    }

    /**
     * Writes a tuple to the output buffer
     *@param buf the tuple written to buffer
     *@return the position of tuple which is in buffer
     *@exception IOException  some I/O fault
     *@exception Exception other exceptions
     */
    public Tuple  Put(Tuple buf)
            throws IOException,
            Exception
    {
        byte[] copybuf;
        copybuf = buf.getTupleByteArray();
        System.arraycopy(copybuf,0,_bufs[curr_page],t_wr_to_pg*t_size,t_size);
        Tuple tuple_ptr = new Tuple(_bufs[curr_page] , t_wr_to_pg * t_size,t_size);

        t_written++; t_wr_to_pg++; t_wr_to_buf++; dirty = true;

        if (t_wr_to_buf == t_in_buf)                // Buffer full?
        {
            flush();                                // Flush it

            t_wr_to_pg = 0; t_wr_to_buf = 0;        // Initialize page info
            curr_page  = 0;
        }
        else if (t_wr_to_pg == t_per_pg)
        {
            t_wr_to_pg = 0;
            curr_page++;
        }

        return tuple_ptr;
    }

    /** Newly developed API
     * Returns the buffer status
     * @return True if buffer is full,
     * False if buffer is not full
     */
    public boolean get_buf_status(){
        return is_buf_full;
    }

    /** Newly developed API
     * Inserts a tuple to the output buffer
     *@param buf the tuple written to buffer
     *@return the position of tuple which is in buffer
     *@exception IOException  some I/O fault
     *@exception Exception other exceptions
     */
    public Tuple insert(Tuple buf)
            throws IOException,
            Exception
    {
        if (t_wr_to_buf == t_in_buf) {
//            System.out.println("^^^^^^^^^^^^^Inserting new candidate to heapfile^^^^^^^^^^^^^^^^^");
            is_buf_full = true;
            // Heapfile implementation for disk
            // storage when buffer is full
            RID rid = _temp_fd.insertRecord(buf.getTupleByteArray());
        }

        else {
            byte[] copybuf;
            copybuf = buf.getTupleByteArray();
            System.arraycopy(copybuf, 0, _bufs[curr_page], t_wr_to_pg * t_size, t_size);
            Tuple tuple_ptr = new Tuple(_bufs[curr_page], t_wr_to_pg * t_size, t_size);

            t_written++;
            t_wr_to_pg++;
            t_wr_to_buf++;
            dirty = true;

            if (t_wr_to_pg == t_per_pg) {
                t_wr_to_pg = 0;
                curr_page++;
            }
            if (t_wr_to_buf == t_in_buf) {
                is_buf_full = true;
            }

            return tuple_ptr;
        }
        return null;
    }

    /** Newly developed API
     * Resets read variables in the buffer to start from beginning of the buffer
     */
    public void reset_read()
        throws IOException,
            Exception
    {
//        System.out.println("Resetting read to start of the buffer");
        t_rd_from_buf = 0;
        t_rd_from_pg = 0;
        rd_curr_page = 0;
    }

    /** Newly developed API
     * Resets write variables in the buffer to start from beginning of the buffer
     */
    public void reset_write()
            throws IOException,
            Exception
    {
//        System.out.println("Resetting read to start of the buffer");
        t_wr_to_buf = 0;
        t_wr_to_pg = 0;
        curr_page = 0;
        is_buf_full=false;
    }

    /** Newly developed API
     * Reads a tuple to the output buffer
     *@return the position of tuple which is in buffer
     *@exception IOException  some I/O fault
     *@exception Exception other exceptions
     */
    public Tuple  Get()
            throws IOException,
            Exception
    {
        if (t_rd_from_buf >= t_wr_to_buf)                // End of buffer?
        {
//            System.out.println("Reached end of the buffer data");
            return null;

        }

        byte[] copybuf = new byte[t_size];
        System.arraycopy(_bufs[rd_curr_page],
                t_rd_from_pg*t_size, copybuf,0, t_size);
        Tuple tuple_ptr = new Tuple(copybuf , 0, t_size);

        t_read++; t_rd_from_pg++; t_rd_from_buf++;

        if (t_rd_from_pg == t_per_pg)
        {
            t_rd_from_pg = 0;
            rd_curr_page++;
        }

        return tuple_ptr;
    }

    /** Newly developed API
     * Deletes a tuple from the buffer
     *@return the position of tuple which is in buffer
     *@exception IOException  some I/O fault
     *@exception Exception other exceptions
     */
    public boolean Delete(int tuple_num)
            throws IOException,
            Exception
    {
        // Implement bit map with a 2D boolean array
        System.out.println("Starting delete of tuple: " + tuple_num);
        while (tuple_num <= (t_wr_to_buf - 2)) {
            int pg_t = (int) Math.ceil(tuple_num/t_per_pg);
            int byte_offset_t = (tuple_num - t_per_pg*(pg_t-1));
            int pg_t2 = (int) Math.ceil((tuple_num+1)/t_per_pg);
            int byte_offset_t2 = ((tuple_num+1) - t_per_pg*(pg_t-1));
            System.arraycopy(_bufs[pg_t2],  byte_offset_t2* t_size,
                    _bufs[pg_t], byte_offset_t * t_size, t_size);
            tuple_num++;
        }
        // Decrement the total tuple delimiters to ignore/remove the last tuple.
        t_wr_to_buf--;
        t_rd_from_buf--;
        // Take care of deletion of first tuples of a page (Roll back)
        if ((t_wr_to_pg == 1) &&
                (t_wr_to_buf > 1)) {
            t_wr_to_pg = t_per_pg;
            t_rd_from_pg = t_per_pg;
        }
        else {
            t_wr_to_pg--;
            t_rd_from_pg--;
        }
        t_written--;
        t_read--;

        System.out.println("Deletion successful");
        return true;
    }



  /**
   * returns the # of tuples written.
   *@return the numbers of tuples written
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public   long flush()  throws IOException, Exception
    {
      int count;
      int bytes_written = 0;
      byte[] tempbuf = new byte[t_size]; 
      if (buffer_only == true)
	System.out.println("Stupid error - but no error protocol");
      
      if (dirty)
	{
	  for (count = 0; count <= curr_page; count++)
	    {
	      RID rid;
	      // Will have to go thru entire buffer writing tuples to disk
	      
	      if (count == curr_page)
		for (int i = 0; i < t_wr_to_pg; i++)
		  {
		    System.arraycopy(_bufs[count],t_size*i,tempbuf,0,t_size);
		    try {
		      rid =  _temp_fd.insertRecord(tempbuf);
		    }
		    catch (Exception e){
		      throw e;
		    }
		  }
	      else
		for (int i = 0; i < t_per_pg; i++)
		  {       
		    System.arraycopy(_bufs[count],t_size*i,tempbuf,0,t_size);
		    try {
		      rid =  _temp_fd.insertRecord(tempbuf);
		    }
		    catch (Exception e){
		      throw e;
		    }
		  }
	    }
	  
	  dirty = false;
	}
      
      return t_written;
    }
  
  private boolean dirty;                                // Does this buffer contain dirty pages?
  private  int  t_per_pg,                        // # of tuples that fit in 1 page
    t_in_buf;                        // # of tuples that fit in the buffer
  private  int  t_wr_to_pg,                        // # of tuples written to current page
    t_wr_to_buf,                        // # of tuples written to buffer.
    t_rd_from_pg,                       // # of tuples read from current page
    t_rd_from_buf;                      // # of tuples read from buffer.
  private  int  curr_page,              // Current page being written to.
    rd_curr_page;                        // Current page being read from.
  private  byte[][]_bufs;                        // Array of pointers to buffer pages.
  private  int  _n_pages;                        // number of pages in array
  private  int  t_size;                                // Size of a tuple
  private  long t_written,                        // # of tuples written so far.
    t_read;                                         // # of tuples read so far.
  private  int  TEST_temp_fd;                        // fd of a temporary file
//    private int total_pg_used,
//    total_tuples_present;
  private  Heapfile _temp_fd;
  private  boolean buffer_only;
  public boolean is_buf_full;               // Flag to indicate if buffer is full
}



