package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.TupleOrder;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
/***
 * 
 * SortFirstSky:: Computes skyline tuples by using sorted tuples as input
 *
 */
public class SortFirstSky extends Iterator {
	
	private AttrType[] in1;
	private int col_len;
	private short[] str_sizes;
	private OBufSortSky oBuf;
	private int number_of_run = 0;
	private FldSpec[] Sprojection;
	private Iterator spScan;
	private Iterator _am1;
	private PageId[] bufs_pids;
	private byte[][] _bufs;
	private int n_pages;
	private boolean first_time = true;
	private int[] pref_list;
	private int pref_list_length;
	
	
	/***
	 * Constructor to initialize necessary details
	 * @param in1				Attribute types for given tuple
	 * @param len_in1			length of attributes
	 * @param t1_str_sizes		Sizes of string
	 * @param am1				Iterator over data file
	 * @param relationName		Name of data relation
	 * @param pref_list			preference attributes
	 * @param pref_list_length	length of preference attributes
	 * @param n_pages			number of pages allowed for this operation
	 * @throws Exception
	 */
	public SortFirstSky(AttrType[] in1, int len_in1, short[] t1_str_sizes,
			Iterator am1, String
			relationName, int[] pref_list, int pref_list_length,
			int n_pages) throws Exception{
		
		this.in1 = in1;
		col_len = len_in1;
		str_sizes = t1_str_sizes;
		_am1 = am1;
		this.pref_list = pref_list;
		this.pref_list_length = pref_list_length;
		
		Sprojection = new FldSpec[len_in1];

		for (int i = 0; i < len_in1; i++) {
			Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		/***
		 * creating sortpref instance, which will give tuples in descending order of the sum of their preference attributes.
		 */
		if(GlobalConst.MAX_SPACE==128) {
			if(n_pages<5) throw new Exception("Not enough pages to run sortFirstSky");
			spScan = new SortPref(in1, (short)len_in1, t1_str_sizes, _am1, new TupleOrder(TupleOrder.Descending), pref_list, pref_list_length, n_pages-4);
		}
		else if(GlobalConst.MAX_SPACE==1024) {
			if(n_pages<4) throw new Exception("Not enough pages to run sortFirstSky");
			spScan = new SortPref(in1, (short)len_in1, t1_str_sizes, _am1, new TupleOrder(TupleOrder.Descending), pref_list, pref_list_length, n_pages-3);
		}
		bufs_pids = new PageId[1];
		_bufs = new byte[1][];
		try {
			/***
			 * getting pages from buffermanager
			 */
			get_buffer_pages(1, bufs_pids, _bufs);
		}catch(Exception e) {
			e.printStackTrace();
		}
		/***
		 * creating a window in the form of a buffer
		 */
		oBuf = new OBufSortSky(in1, (short)len_in1, t1_str_sizes, _bufs, pref_list, pref_list_length, 1);
	}

	
	/****
	 * Iterates over sorted tuples and finds one skyline tuple at a time, if there is no more skyline tuple, it will return null
	 */
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		Tuple t;

		/***
		 * Iterating over outer loop which is sorted
		 */
		while ((t = spScan.get_next()) != null) {
			
			/***
			 * checking if the tuple is getting dominated by any existing tuple inside the buffer.
			 * if no tuple from the buffer dominates this tuple t, we insert t into the buffer and
			 * return it.
			 */
			if (oBuf.checkIfSky(t)) {
				t = oBuf.Put(t);
				if(oBuf.isFlag()) {
					continue;
				}
				return t;
			}
		}
		/***
		 * If one outer scan is completed, we need to check if buffer is full or not,
		 * if buffer is full, we need to make the heapfile(where buffer was writing overflowed tuples)
		 * our new outer scan.
		 */
		if (oBuf.isFlag()) {
			spScan.close();
			if(first_time) {
				if(n_pages>100) {
					try {
						/***
						 * Freeing pages
						 */
						free_buffer_pages(1, bufs_pids);
						
					}catch(Exception e) {
						
					}
					try {
						/***
						 * getting pages from buffermanager
						 */
						bufs_pids = new PageId[n_pages/2];
						_bufs = new byte[n_pages/2][];
						get_buffer_pages(n_pages/2, bufs_pids, _bufs);
						oBuf = new OBufSortSky(in1, (short)col_len, str_sizes, _bufs, pref_list, pref_list_length, n_pages/2);
					}catch(Exception e) {
						e.printStackTrace();
					}
				}
				first_time = false;
			}
			spScan = new FileScan(oBuf.getCurr_file() + number_of_run, in1, str_sizes, (short)col_len, col_len, Sprojection, null);
			if (number_of_run > 0) {
				new Heapfile(oBuf.getCurr_file() + (number_of_run - 1)).deleteFile();
			}
			number_of_run++;
			oBuf.setNumber_of_window_file(oBuf.getNumber_of_window_file()+1);
			oBuf.setFlag(false);
			oBuf.init();
			return get_next();
		}
		return null;
	}
/***
 * closing the scans and deleting heap files.
 */
	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		spScan.close();
		try {
			if(first_time) n_pages=2;
			free_buffer_pages(n_pages/2, bufs_pids);
			if(number_of_run>0)
				new Heapfile(oBuf.getCurr_file() + (number_of_run - 1)).deleteFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
