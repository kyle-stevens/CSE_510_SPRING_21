package iterator;

import java.io.IOException;

import global.AttrType;
import global.GlobalConst;
import global.PageId;
import heap.Heapfile;
import heap.Tuple;

/***
 * 
 * OBufSortSky::Represents the concept of window in skyline operators like BtreeSortedSky and SortFirstSky.
 *
 */

public class OBufSortSky implements GlobalConst{
	
	private int t_per_pg, // # of tuples that fit in 1 page
			t_in_buf; // # of tuples that fit in the buffer
	private int t_wr_to_pg = 0, // # of tuples written to current page
			t_wr_to_buf = 0; // # of tuples written to buffer.
	private int curr_page = 0; // Current page being written to.
	private byte[][] _bufs; // Array of pointers to buffer pages.
	private int _n_pages; // number of pages in array
	private int t_size; // Size of a tuple

	private AttrType[] in1; // Attribute types of the tuple
	private short col_len;	// No of columns in the tuple
	private short[] str_sizes; // Sizes of strings in the tuple
	private int[] pref_list;	// List of preference attributes
	private int pref_list_length;	// number of preference attributes.
	final private String curr_file = "curr_file";	//name of the heapfile(to which overflowed tuples get stored)
	private int number_of_window_file = 0;	//Keep track of number of runs
	private boolean flag = false;
	
	
	public int getNumber_of_window_file() {
		return number_of_window_file;
	}

	public void setNumber_of_window_file(int number_of_window_file) {
		this.number_of_window_file = number_of_window_file;
	}

	public String getCurr_file() {
		return curr_file;
	}

	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}

	
	/***
	 * Constructor to initialize necessary details
	 * @param in1			Attribute types for given tuple
	 * @param len_in1		Number of columns
	 * @param t1_str_sizes	sizes of string in given tuple
	 * @param buf			Buffer which will be used to implement the window of n_pages
	 * @param pref_list		List of preference attributes
	 * @param pref_list_length		numner of preference attributes
	 * @param n_pages		Size of the buffer array which represents available pages in the main memory
	 * @throws SortException
	 */
	public OBufSortSky(AttrType[] in1, short len_in1, short[] t1_str_sizes, byte[][] buf,
			int[] pref_list, int pref_list_length, int n_pages) throws SortException{
		
		this.in1 = in1;
		System.out.println("len = "+in1.length);
		col_len = len_in1;
		str_sizes = t1_str_sizes;
		this.pref_list = pref_list;
		this.pref_list_length = pref_list_length;
		_bufs = buf;
		Tuple t = new Tuple();
		
		try {
			t.setHdr(col_len, this.in1, str_sizes);
		} catch (Exception e) {
			throw new SortException(e, "Sort.java: t.setHdr() failed");
		}
		t_size = t.size();
		_n_pages = n_pages;
		t_per_pg = MINIBASE_PAGESIZE / t_size;
		t_in_buf = _n_pages * t_per_pg;
		init();
	}

	
	/***
	 * initializing attribute values
	 */
	public void init() {
		t_wr_to_pg = 0;
		t_wr_to_buf = 0;
		curr_page = 0;
	}
	
	
	/***
	 * checks if tuple can be in the skyline or not by comparing it to each of the tuple in the buffer
	 * @param t tuple which needs to be checked
	 * @return	true if tuple can be in the skyline, false otherwise.
	 * @throws Exception
	 */
	public boolean checkIfSky(Tuple t) throws Exception {

		for (int count = 0; count <= curr_page; count++) {
			int len = t_per_pg;

			if (count == curr_page)
				len = t_wr_to_pg;

			for (int i = 0; i < len; i++) {
				try {
					Tuple t2 = new Tuple(_bufs[count], t_size * i, t_size);
					t2.setHdr(col_len, in1, str_sizes);
					if (TupleUtils.Dominates(t2, in1, t, in1, col_len, str_sizes, pref_list, pref_list_length))
						return false;
				} catch (Exception e) {
					throw e;
				}
			}
		}

		return true;
	}

	
	/***
	 * inserts a tuple into a buffer, if buffer is full, inserts the tuple into a heapfile
	 * @param buf tuple which needs to be inserted in the buffer
	 * @return	copy of the tuple after inserting it. 
	 * @throws IOException
	 * @throws Exception
	 */
	public Tuple Put(Tuple buf) throws IOException, Exception {

		
		
		byte[] copybuf;
		copybuf = buf.getTupleByteArray();
		if (t_wr_to_buf == t_in_buf) // Buffer full?
		{
			/***
			 * if the buffer is full, we insert new records into a heapfile.
			 */
			Heapfile f = new Heapfile(curr_file + number_of_window_file);
			f.insertRecord(copybuf);
			flag = true;
			return buf;
		}
		System.arraycopy(copybuf, 0, _bufs[curr_page], t_wr_to_pg * t_size, t_size);
		Tuple tuple_ptr = new Tuple(_bufs[curr_page], t_wr_to_pg * t_size, t_size);
		tuple_ptr.setHdr(col_len, in1, str_sizes);
		t_wr_to_pg++;
		t_wr_to_buf++;

		if (t_wr_to_pg == t_per_pg) {
			t_wr_to_pg = 0;
			curr_page++;
		}
		return tuple_ptr;
	}
}