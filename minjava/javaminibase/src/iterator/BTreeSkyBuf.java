package iterator;

import java.io.IOException;

import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

public class BTreeSkyBuf implements GlobalConst {

	Heapfile _blockNestedFile;
	Heapfile _dataFile;
	
	/**
	   *Default constructor
	   * no args -- use init to initialize
	   */
	  public BTreeSkyBuf(){}
	  AttrType[] _in;


	  /**
	   * O_buf is an output buffer. It takes as input:
	   *@param bufs  temporary buffer to pages.(EACH ELEMENT IS A SINGLE BUFFER PAGE).
	   *@param n_pages the number of pages
	   *@param tSize   tuple size
	   *@param temp_fd  fd of a  HeapFile
	   *@param buffer  true => it is used as a buffer => if it is flushed, print
	   *                      a nasty message. it is false by default.
	  */
	  public BTreeSkyBuf( byte[][] bufs, int n_pages,
			   Heapfile temp_fd ,Heapfile blockNestedFile, Heapfile dataFile)
	    {
		  _bufs    = bufs;
	      _n_pages = n_pages;
	      _temp_fd = temp_fd;
	      _blockNestedFile = blockNestedFile;
	      _dataFile = dataFile;
	      
	      _in = new AttrType[2];
	      for(int j = 0; j < 2; j++)
		      _in[j] = new AttrType(AttrType.attrInteger);
	      
	      Tuple t = new Tuple();
		    try {
		      t.setHdr((short) 2, _in, null);
		    } catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      e.printStackTrace();
		    }
		
		  t_size = t.size();
	      
	      dirty       = false;
	      t_per_pg    = MINIBASE_PAGESIZE / t_size;
	      t_in_buf    = _n_pages * t_per_pg;
	      t_wr_to_pg  = 0;
	      t_wr_to_buf = 0;
	      t_written   = 0L;
	      curr_page   = 0;
	      is_buf_full = false;
	    }

	    /**
	     * Writes a tuple to the output buffer
	     *@param buf the tuple written to buffer
	     *@return the position of tuple which is in buffer
	     *@exception IOException  some I/O fault
	     *@exception Exception other exceptions
	     */
	    public boolean Put(RID t)
	            throws IOException,
	            Exception
	    {
	    	Tuple buf = tupleFromRid(t);
	    	if(checkIfDuplicate(buf)) return false;
	    	
	        byte[] copybuf;
	        copybuf = buf.getTupleByteArray();
	        System.arraycopy(copybuf,0,_bufs[curr_page],t_wr_to_pg*t_size,t_size);
	        
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
	        _blockNestedFile.insertRecord(_dataFile.getRecord(t).getTupleByteArray());
	        
	        return true;
	    }

	 
	   
	    public boolean checkIfDuplicate(Tuple t) throws Exception {
	    	
			for (int count = 0; count <= curr_page; count++) {
				int len = t_per_pg;

				if (count == curr_page)
					len = t_wr_to_pg;

				for (int i = 0; i < len; i++) {
					try {
						Tuple t2 = new Tuple(_bufs[count], t_size * i, t_size);
						
						t2.setHdr((short)2, _in, null);
						if (TupleUtils.Equal(t, t2, _in, 2))
							return true;
					} catch (Exception e) {
						throw e;
					}
				}
			}
			Scan scan = new Scan(_temp_fd);
			Tuple t2 = null;
			while((t2=scan.getNext(new RID()))!=null) {
				t2.setHdr((short)2, _in, null);
				if (TupleUtils.Equal(t, t2, _in, 2)) {
					scan.closescan();
					return true;
				}
			}
			scan.closescan();
			return false;
		}
	   
	    private Tuple tupleFromRid(RID rid) throws Exception {
			
		   
		    Tuple t = new Tuple(t_size);
		    try {
		      t.setHdr((short) 2, _in, null);
		    } catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      e.printStackTrace();
		    }
		    t.setIntFld(1, rid.pageNo.pid);
		    t.setIntFld(2, rid.slotNo);
		    return t;
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
	      byte[] tempbuf = new byte[t_size]; 
	      
	      if (dirty)
		{
		  for (count = 0; count <= curr_page; count++)
		    {
		      // Will have to go thru entire buffer writing tuples to disk
		      
		      if (count == curr_page)
			for (int i = 0; i < t_wr_to_pg; i++)
			  {
			    System.arraycopy(_bufs[count],t_size*i,tempbuf,0,t_size);
			    try {
			       _temp_fd.insertRecord(tempbuf);
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
			      _temp_fd.insertRecord(tempbuf);
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
	    t_wr_to_buf;                     // # of tuples read from buffer.
	  private  int  curr_page;                        // Current page being written to.
	  private  byte[][]_bufs;                        // Array of pointers to buffer pages.
	  private  int  _n_pages;                        // number of pages in array
	  private  int  t_size;                                // Size of a tuple
	  private  long t_written;
	  private  Heapfile _temp_fd;
	  public boolean is_buf_full;

}
