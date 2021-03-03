package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.PageId;
import global.TupleOrder;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

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
	
	public SortFirstSky(AttrType[] in1, int len_in1, short[] t1_str_sizes,
			Iterator am1, String
			relationName, int[] pref_list, int pref_list_length,
			int n_pages) throws Exception{
		this.in1 = in1;
		col_len = len_in1;
		str_sizes = t1_str_sizes;
		_am1 = am1;
		
		Sprojection = new FldSpec[len_in1];

		for (int i = 0; i < len_in1; i++) {
			Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		bufs_pids = new PageId[n_pages];
		
		spScan = new SortPref(in1, (short)len_in1, t1_str_sizes, _am1, new TupleOrder(TupleOrder.Descending), pref_list, pref_list_length, n_pages);
		_bufs = new byte[n_pages][];
		try {
			get_buffer_pages(n_pages, bufs_pids, _bufs);
		}catch(Exception e) {
			e.printStackTrace();
		}
		oBuf = new OBufSortSky(in1, (short)len_in1, t1_str_sizes, _bufs, pref_list, pref_list_length, n_pages);
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		Tuple t;

		while ((t = spScan.get_next()) != null) {
			if (oBuf.checkIfSky(t)) {
				return oBuf.Put(t);
			}
		}
		if (oBuf.isFlag()) {
			spScan.close();
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

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		spScan.close();
		try {
			new Heapfile(oBuf.getCurr_file() + (number_of_run - 1)).deleteFile();
		} catch (Exception e) {

		}
	}
}
