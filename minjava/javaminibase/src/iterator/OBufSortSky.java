package iterator;

import java.io.IOException;

import global.AttrType;
import global.GlobalConst;
import heap.Heapfile;
import heap.Tuple;

public class OBufSortSky implements GlobalConst{
	
	private int t_per_pg, // # of tuples that fit in 1 page
			t_in_buf; // # of tuples that fit in the buffer
	private int t_wr_to_pg = 0, // # of tuples written to current page
			t_wr_to_buf = 0; // # of tuples written to buffer.
	private int curr_page = 0; // Current page being written to.
	private byte[][] _bufs; // Array of pointers to buffer pages.
	private int _n_pages; // number of pages in array
	private int t_size; // Size of a tuple

	private AttrType[] in1;
	private short col_len;
	private short[] str_sizes;
	private int[] pref_list;
	private int pref_list_length;
	final private String curr_file = "curr_file";
	private int number_of_window_file = 0;
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

	public OBufSortSky(AttrType[] in1, short len_in1, short[] t1_str_sizes, Iterator am1, String relationName,
			int[] pref_list, int pref_list_length, String index_file, int n_pages) throws SortException{
		
		this.in1 = in1;
		System.out.println("len = "+in1.length);
		col_len = len_in1;
		str_sizes = t1_str_sizes;
		this.pref_list = pref_list;
		this.pref_list_length = pref_list_length;
		
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
		_bufs = new byte[_n_pages][];
		init();
	}

	public void init() {
		t_wr_to_pg = 0;
		t_wr_to_buf = 0;
		curr_page = 0;
		for (int k = 0; k < _n_pages; k++)
			_bufs[k] = new byte[MAX_SPACE];
	}
	
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

	public Tuple Put(Tuple buf) throws IOException, Exception {

		byte[] copybuf;
		copybuf = buf.getTupleByteArray();
		System.arraycopy(copybuf, 0, _bufs[curr_page], t_wr_to_pg * t_size, t_size);
		Tuple tuple_ptr = new Tuple(_bufs[curr_page], t_wr_to_pg * t_size, t_size);
		tuple_ptr.setHdr(col_len, in1, str_sizes);
		t_wr_to_pg++;
		t_wr_to_buf++;

		if (t_wr_to_buf == t_in_buf) // Buffer full?
		{
			Heapfile f = new Heapfile(curr_file + number_of_window_file);
			f.insertRecord(copybuf);
			flag = true;
		} else if (t_wr_to_pg == t_per_pg) {
			t_wr_to_pg = 0;
			curr_page++;
		}
		return tuple_ptr;
	}
}