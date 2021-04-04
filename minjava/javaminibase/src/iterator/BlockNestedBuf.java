package iterator;

import java.io.IOException;

import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

/***
 * 
 * BlockNestedBuf::Represents the concept of window in BlockNested skyline operators.
 *
 */

public class BlockNestedBuf implements GlobalConst{
	
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
	final private String curr_file = "curr_file_block";	//name of the heapfile(to which overflowed tuples get stored)
	private int number_of_window_file = 0;	//Keep track of number of runs
	private boolean flag = false;
	private Heapfile hf;
	
	
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
	public BlockNestedBuf(AttrType[] in1, short len_in1, short[] t1_str_sizes, byte[][] buf,
			int[] pref_list, int pref_list_length, int n_pages) throws SortException{
		
		col_len = (short) (len_in1+1);	// 1 extra attr for marking 'deleted' => (int) 4 bytes of extra space for each tuple in the buffer
		str_sizes = t1_str_sizes;
		this.pref_list = pref_list;
		this.pref_list_length = pref_list_length;
		_bufs = buf;
		_n_pages = n_pages;
		this.in1 = new AttrType[col_len];
		for (int i = 0; i < len_in1; i++)
			this.in1[i] = in1[i];
		this.in1[col_len-1] = new AttrType(AttrType.attrInteger);	// additional attr for deleted flag is of type int.
		
		Tuple t = new Tuple();
		try {
			t.setHdr((short) col_len, this.in1, null);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		t_size = t.size();
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
		read_count=0;
		pageLen=t_per_pg;
		tuple_count=0;
	}
	
	
	
	
	/***
	 * checks if tuple can be in the skyline or not by comparing it to each of the tuple in the buffer
	 * @param t tuple which needs to be checked
	 * @return	true if tuple can be in the skyline, false otherwise.
	 * @throws Exception
	 */
	public boolean checkIfDominates(Tuple t) throws Exception {
		AttrType[] in2 = new AttrType[col_len-1];
		int k=0;
		for(AttrType attr:in1) {
			in2[k++]=attr;
			if(k==col_len-1)break;
		}
		// loop on available pages in the buffer
		for (int count = 0; count <= curr_page; count++) {
			int len = t_per_pg;

			if (count == curr_page)
				len = t_wr_to_pg;

			// loop on available tuples on a buffer page
			for (int i = 0; i < len; i++) {
				try {
					Tuple t2 = new Tuple(_bufs[count], t_size * i, t_size);
					t2.setHdr(col_len, in1, str_sizes);
					
					if (t2.getIntFld(col_len)==0)	// Skip tuple if it is marked as deleted.
					{
						if(TupleUtils.Dominates(t, in1, t2, in1, col_len, str_sizes, pref_list, pref_list_length)) {

							t2.setIntFld(col_len, 1);	// mark tuple as deleted
							byte[] copybuf = t2.getTupleByteArray();
							System.arraycopy(copybuf, 0, _bufs[count], i * t_size, t_size);	// write updated tuple into buffer
						}
						else if(TupleUtils.Dominates(t2, in1, t, in1, col_len, str_sizes, pref_list, pref_list_length)) {
							//	end comparison if t gets dominated
							return false;
						}
					}
				} catch (Exception e) {
					throw e;
				}
			}
		}
		blockPut(t);	// Adds t as skyline candidate
		return true;
	}
	
	
	
	
	int read_count=0;	// read page counter
	int pageLen = t_per_pg;
	int tuple_count=0;
	int j=0;

	/***
	 * Returns skyline tuple
	 * @return	Tuple if it exists, Null if not.
	 */
	public Tuple getSkyTuple(){
		//if(j++==0)System.out.println(read_count+" "+tuple_count);
		if(read_count<=curr_page) {
			if(read_count==curr_page) {
				pageLen = t_wr_to_pg;	// To indicate end of tuples in the last page
			}
			if(tuple_count<pageLen) {
				try {
					//System.out.print(j+++" ");
					Tuple t2 = new Tuple(_bufs[read_count], t_size * tuple_count, t_size);
					t2.setHdr(col_len, in1, str_sizes);
					//t2.print(in1);
					tuple_count++;
					if(tuple_count==pageLen) {
						tuple_count=0;
						read_count++;
					}
					if (t2.getIntFld(col_len)==0)
					{
						return createTupleBack(t2);	// return tuple with normal header
					}else return getSkyTuple();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	/**
	 * Inserts the passed tuple into buffer if its free or a temporary heap file if its full
	 * @param buf	Tuple to be added
	 * @return	added Tuple
	 * @throws IOException
	 * @throws Exception
	 */
	public Tuple blockPut(Tuple buf) throws IOException, Exception {

		byte[] copybuf;
		copybuf = buf.getTupleByteArray();
		if (flag||(t_wr_to_buf == t_in_buf)) // Buffer full?
		{
			/***
			 * if the buffer is full, we insert new records into a heapfile.
			 */
			Heapfile f = new Heapfile(curr_file + number_of_window_file);
			f.insertRecord(copybuf);	// Uses 2 Pages
			flag = true;	// Mark buffer as full if not already marked
			return buf;
		}
		Tuple temp = createTuple(buf);	// Temporary tuple with extra attr for 'deleted' flag
		copybuf = temp.getTupleByteArray();
		if(curr_page==1) {
//			System.out.println(t_wr_to_buf+" "+t_in_buf+" "+t_per_pg+" "+t_wr_to_pg);
		}
		System.arraycopy(copybuf, 0, _bufs[curr_page], t_wr_to_pg * t_size, t_size);
		t_wr_to_pg++;
		t_wr_to_buf++;

		if (t_wr_to_pg == t_per_pg) {
			t_wr_to_pg = 0;
			curr_page++;
		}
		return buf;
	}

	/**
	 * Creates a new tuple with additional ('deleted') attribute
	 * @param t1 Tuple to update
	 * @return Updated tuple with one extra attribute of type int
	 */
	private Tuple createTuple(Tuple t1) {
		
		Tuple t = new Tuple(t_size);
		try {
			t.setHdr((short) col_len, in1, null);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		for(int i=1;i<col_len;i++) {
			try {
				t.setFloFld(i, t1.getFloFld(i));
				
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			t.setIntFld(col_len, 0);
		} catch (FieldNumberOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return t;
	}

	/**
	 * Removes additional ('deleted') attr from the tuple passed
	 * @param t1 Tuple to revert back
	 * @return	Tuple without additional attr
	 */
	private Tuple createTupleBack(Tuple t1) {
		AttrType[] in2 = new AttrType[col_len-1];
		int k=0;
		for(AttrType attr:in1) {
			in2[k++]=attr;
			if(k==col_len-1)break;
		}
		Tuple t = new Tuple();
		try {
			t.setHdr((short) (col_len-1), in2, null);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		t = new Tuple(t.size());
		try {
			t.setHdr((short) (col_len-1), in2, null);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		for(int i=1;i<col_len;i++) {
			try {
				t.setFloFld(i, t1.getFloFld(i));
				
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return t;
	}
	
}