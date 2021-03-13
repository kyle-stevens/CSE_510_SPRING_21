package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.IndexType;
import global.PageId;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;

/***
 * BTreeSortedSky::Performs the skyline operation on a combined index file
 * and returns the result.
 *
 */
public class BTreeSortedSky extends Iterator {

	
	private AttrType[] in1;
	private short col_len;
	private short[] str_sizes;
	private Iterator scan;
	private FldSpec[] Sprojection;
	private OBufSortSky oBuf;
	private int number_of_run = 0;
	private PageId[] bufs_pids;
	private byte[][] _bufs;

	/***
	 * Constructor to initialize necessary details.
	 * @param in1				Attribute types for given tuple
	 * @param len_in1			length of attributes
	 * @param t1_str_sizes		Sizes of string
	 * @param am1				Iterator over data file
	 * @param relationName		Name of data relation
	 * @param pref_list			preference attributes
	 * @param pref_list_length	length of preference attributes
	 * @param index_file		Name of the combined index file
	 * @param n_pages			Number of pages available to us for this operation
	 * @throws Exception 
	 */
	public BTreeSortedSky(AttrType[] in1, short len_in1, short[] t1_str_sizes, Iterator am1, String relationName,
			int[] pref_list, int pref_list_length, String index_file, int n_pages) throws Exception {
		n_pages-=4; //reserving 2 pages for file scan and 2 pages for getting record from indexscan and creating new heap files
		if(n_pages<1)
			throw new Exception("Not enough pages to compute the skyline");
		this.in1 = in1;
		col_len = len_in1;
		str_sizes = t1_str_sizes;
		n_pages = Math.min(10,n_pages/2);
		if(n_pages<1)n_pages=1;
		bufs_pids = new PageId[n_pages];
		_bufs = new byte[n_pages][];
		/***
		 * getting pages from buffermanager
		 */
		get_buffer_pages(n_pages, bufs_pids, _bufs);
		/***
		 * creating a window in the form of a buffer
		 */
		oBuf = new OBufSortSky(in1, len_in1, t1_str_sizes, _bufs, pref_list, pref_list_length, n_pages);

		Sprojection = new FldSpec[len_in1];

		for (int i = 0; i < len_in1; i++) {
			Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		/***
		 * creating Index scan on already created indexfile, 
		 * which will give tuples in descending order of the sum of their preference attributes.
		 */
		scan = new IndexScan(new IndexType(IndexType.B_Index), relationName, index_file, this.in1, str_sizes, col_len,
				col_len, Sprojection, null, 0, false);

	}
	
	/****
	 * Iterates over sorted tuples and finds one skyline tuple at a time, if there is no more skyline tuple, it will return null
	 */
	int i=0;
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		Tuple t;

		/***
		 * Iterating over outer loop which is sorted
		 */
		while ((t = scan.get_next()) != null) {
			/***
			 * checking if the tuple is getting dominated by any existing tuple inside the buffer.
			 * if no tuple from the buffer dominates this tuple t, we insert t into the buffer and
			 * return it.
			 */
			if (oBuf.checkIfSky(t)) {
				return oBuf.Put(t);
			}
		}
		/***
		 * If one outer scan is completed, we need to check if buffer is full or not,
		 * if buffer is full, we need to make the heapfile(where buffer was writing overflowed tuples)
		 * our new outer scan.
		 */
		if (oBuf.isFlag()) {
			scan.close();
			scan = new FileScan(oBuf.getCurr_file() + number_of_run, in1, str_sizes, col_len, col_len, Sprojection, null);
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
		scan.close();
		try {
			new Heapfile(oBuf.getCurr_file() + (number_of_run - 1)).deleteFile();
		} catch (Exception e) {

		}
	}

}


