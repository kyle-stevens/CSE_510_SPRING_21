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
	 * @param in1	
	 * @param len_in1
	 * @param t1_str_sizes
	 * @param am1
	 * @param relationName
	 * @param pref_list
	 * @param pref_list_length
	 * @param index_file
	 * @param n_pages
	 * @throws IndexException
	 * @throws InvalidTypeException
	 * @throws InvalidTupleSizeException
	 * @throws UnknownIndexTypeException
	 * @throws IOException
	 * @throws SortException
	 * @throws IteratorBMException
	 */
	public BTreeSortedSky(AttrType[] in1, short len_in1, short[] t1_str_sizes, Iterator am1, String relationName,
			int[] pref_list, int pref_list_length, String index_file, int n_pages) throws IndexException,
			InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException, IOException, SortException, IteratorBMException {
		n_pages-=4; //reserving 2 pages for file scan and 2 pages for getting record from indexscan and creating new heap files
		this.in1 = in1;
		col_len = len_in1;
		str_sizes = t1_str_sizes;
		bufs_pids = new PageId[n_pages];
		_bufs = new byte[n_pages][];
		get_buffer_pages(n_pages, bufs_pids, _bufs);
		oBuf = new OBufSortSky(in1, len_in1, t1_str_sizes, _bufs, pref_list, pref_list_length, n_pages);

		Sprojection = new FldSpec[len_in1];

		for (int i = 0; i < len_in1; i++) {
			Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}

		scan = new IndexScan(new IndexType(IndexType.B_Index), relationName, index_file, this.in1, str_sizes, col_len,
				col_len, Sprojection, null, 0, false);

	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		Tuple t;

		while ((t = scan.get_next()) != null) {
			if (oBuf.checkIfSky(t)) {
				return oBuf.Put(t);
			}
		}
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


