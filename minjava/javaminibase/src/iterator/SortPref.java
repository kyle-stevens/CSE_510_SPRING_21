package iterator;

import java.io.IOException;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;


/***
 * SortPref::This class sorts the tuples with respect to the sum of their preference attributes.
 * Used for computing the skyline attributes.
 */
public class SortPref extends Iterator implements GlobalConst {
	private static final int ARBIT_RUNS = 10;

	private AttrType[] _in;
	private short n_cols;
	private short[] str_lens;
	private Iterator _am;
	private TupleOrder order;
	private int _n_pages;
	private int Nruns;
	private int max_elems_in_heap;
	private int tuple_size;

	private pnodeSplayPQ Q;
	private Heapfile[] temp_files;
	private int n_tempfiles;
	private Tuple output_tuple;
	private int[] n_tuples;
	private int n_runs;
	private Tuple op_buf;
	private Scan[] i_buf;
	
	private int[] pref_list;
	private int pref_list_len;
	
	private static final String tmp_file_prefix = "TMP_SORT_";

	
	/***
	 * Constructor to initialize the necessary details.
	 * @param in Attribute types of tuple t1
	 * @param len_in number of columns in both the tuple
	 * @param str_sizes sizes of strings types in the tuples
	 * @param am iterator on the file which needs to be sorted.
	 * @param sort_order specify if ascending or descending
	 * @param pref_list preference attributes list
	 * @param pref_list_length length of the preference list.
	 * @param n_pages number of pages available for sorting.
	 * @throws Exception 
	 * @throws IOException 
	 * @throws JoinsException 
	 * @throws TupleUtilsException 
	 * @throws UnknowAttrType 
	 */
	public SortPref(AttrType[] in, short len_in, short[] str_sizes, Iterator am, TupleOrder sort_order,
			int[] pref_list, int pref_list_len, int n_pages) throws UnknowAttrType, TupleUtilsException, JoinsException, IOException, Exception {
		if(n_pages<2) throw new Exception("Not enough pages to sort the data.");
		n_cols = len_in;
		this.pref_list = pref_list;
		this.pref_list_len = pref_list_len;
		_am = am;
		order = sort_order;
		_n_pages = n_pages;
		_in = in.clone();
		str_lens = str_sizes.clone();


		Tuple t = new Tuple();
		try {
			t.setHdr(len_in, _in, str_sizes);
		} catch (Exception e) {
			throw new SortException(e, "Sort.java: t.setHdr() failed");
		}
		tuple_size = t.size();

		//Initial arbitrary number of runs, which can be updated in later stages according to the data
		temp_files = new Heapfile[ARBIT_RUNS];
		n_tempfiles = ARBIT_RUNS;
		n_tuples = new int[ARBIT_RUNS];
		n_runs = ARBIT_RUNS;

		try {
			temp_files[0] = new Heapfile(tmp_file_prefix+0);	// heapfile to store the tuples of specific run
		} catch (Exception e) {
			throw new SortException(e, "Sort.java: Heapfile error");
		}
		
		//No of maximum elements the are allowed inside a heap at a time
		max_elems_in_heap = 200;

		//Creating a priority queue which helps in generating sorted runs
		Q = new pnodeSplayPQ(order,this.pref_list,this.pref_list_len,_in,n_cols,str_lens); 

		
		op_buf = new Tuple(tuple_size);
		try {
			op_buf.setHdr(n_cols, _in, str_lens); 
		} catch (Exception e) {
			throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
		}
		
		// generate runs, needs 2 unpinned pages to create and insert into the heapfiles
		Nruns = generate_runs(max_elems_in_heap);
		
		_am.close();
		_n_pages +=2;	//iterator scan is closed now
		
		// setup state to perform merge of runs.
		// Open input buffers for all the input file
		setup_for_merge(tuple_size, Nruns);
	}
	
	/**
	 * Generate sorted runs. Using heap sort.
	 * 
	 * @param max_elems   maximum number of elements in heap
	 * @param sortFldType attribute type of the sort field
	 * @param sortFldLen  length of the sort field
	 * @return number of runs generated
	 * @exception IOException    from lower layers
	 * @exception SortException  something went wrong in the lower layer.
	 * @exception JoinsException from <code>Iterator.get_next()</code>
	 */
	private int generate_runs(int max_elems)
			throws IOException, SortException, UnknowAttrType, TupleUtilsException, JoinsException, Exception {
		Tuple tuple;
		pnode cur_node;
		pnodeSplayPQ pcurr_Q = new pnodeSplayPQ(order,pref_list,pref_list_len,_in,n_cols,str_lens);
		pnodeSplayPQ pother_Q = new pnodeSplayPQ(order,pref_list,pref_list_len,_in,n_cols,str_lens);
		
		double lastSum = Double.MIN_VALUE;

		int run_num = 0; // keeps track of the number of runs

		// number of elements in Q
		int p_elems_curr_Q = 0;
		int p_elems_other_Q = 0;

		
		int comp_res;

		// set the lastElem to be the minimum value for the sort field
		if (order.tupleOrder == TupleOrder.Descending) {
			lastSum = Double.MAX_VALUE;
		} 

		// maintain a fixed maximum number of elements in the heap
		while ( p_elems_curr_Q  < max_elems) {
			try {
				tuple = _am.get_next();
			} catch (Exception e) {
				e.printStackTrace();
				throw new SortException(e, "Sort.java: get_next() failed");
			}

			if (tuple == null) {
				break;
			}
			cur_node = new pnode();
			cur_node.tuple = new Tuple(tuple); // tuple copy needed -- Bingjie 4/29/98

			pcurr_Q.enqPref(cur_node);
			p_elems_curr_Q++;
		}

		// now the queue is full, starting writing to file while keep trying
		// to add new tuples to the queue. The ones that does not fit are put
		// on the other queue temporarily
		while (true) {
			cur_node = pcurr_Q.deqPref();
			if (cur_node == null)
				break;
			p_elems_curr_Q--;

			comp_res = TupleUtils.CompareTupleWithValuePref(lastSum, cur_node.tuple,_in,n_cols,pref_list,pref_list_len); // need
																											// tuple_utils.java

			if ((comp_res > 0 && order.tupleOrder == TupleOrder.Ascending)
					|| (comp_res < 0 && order.tupleOrder == TupleOrder.Descending)) {
				// doesn't fit in current run, put into the other queue
				try {
					pother_Q.enqPref(cur_node);
				} catch (UnknowAttrType e) {
					throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enqPref()");
				}
				p_elems_other_Q++;
			} else {
				// set lastElem to have the value of the current tuple,
				// need tuple_utils.java
				lastSum = TupleUtils.getPrefAttrSum(cur_node.tuple, _in, n_cols, pref_list, pref_list_len);
				// write tuple to output file, need io_bufs.java, type cast???
				// System.out.println("Putting tuple into run " + (run_num + 1));
				// cur_node.tuple.print(_in);
				temp_files[run_num].insertRecord(cur_node.tuple.getTupleByteArray());
				n_tuples[run_num]++;
			}

			// check whether the other queue is full
			if (p_elems_other_Q == max_elems) {
				// close current run and start next run
				run_num++;

				// check to see whether need to expand the array
				if (run_num == n_tempfiles) {
					Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
					for (int i = 0; i < n_tempfiles; i++) {
						temp1[i] = temp_files[i];
					}
					temp_files = temp1;
					n_tempfiles *= 2;

					int[] temp2 = new int[2 * n_runs];
					for (int j = 0; j < n_runs; j++) {
						temp2[j] = n_tuples[j];
					}
					n_tuples = temp2;
					n_runs *= 2;
				}

				try {
					temp_files[run_num] = new Heapfile(tmp_file_prefix+run_num);
				} catch (Exception e) {
					throw new SortException(e, "Sort.java: create Heapfile failed");
				}

				// set the last Elem to be the minimum value for the sort field
				if (order.tupleOrder == TupleOrder.Ascending) {
					lastSum = Double.MIN_VALUE;
				} else {
					lastSum = Double.MAX_VALUE;
				}

				// switch the current heap and the other heap
				pnodeSplayPQ tempQ = pcurr_Q;
				pcurr_Q = pother_Q;
				pother_Q = tempQ;
				int tempelems = p_elems_curr_Q;
				p_elems_curr_Q = p_elems_other_Q;
				p_elems_other_Q = tempelems;
			}

			// now check whether the current queue is empty
			else if (p_elems_curr_Q == 0) {
				while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
					try {
						tuple = _am.get_next();
					} catch (Exception e) {
						throw new SortException(e, "get_next() failed");
					}

					if (tuple == null) {
						break;
					}
					cur_node = new pnode();
					cur_node.tuple = new Tuple(tuple); // tuple copy needed -- Bingjie 4/29/98

					try {
						pcurr_Q.enqPref(cur_node);
					} catch (UnknowAttrType e) {
						throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enqPref()");
					}
					p_elems_curr_Q++;
				}
			}

			// Check if we are done
			if (p_elems_curr_Q == 0) {
				// current queue empty despite our attempts to fill in
				// indicating no more tuples from input
				if (p_elems_other_Q == 0) {
					// other queue is also empty, no more tuples to write out, done
					break; // of the while(true) loop
				} else {
					// generate one more run for all tuples in the other queue
					// close current run and start next run
					run_num++;

					// check to see whether need to expand the array
					if (run_num == n_tempfiles) {
						Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
						for (int i = 0; i < n_tempfiles; i++) {
							temp1[i] = temp_files[i];
						}
						temp_files = temp1;
						n_tempfiles *= 2;

						int[] temp2 = new int[2 * n_runs];
						for (int j = 0; j < n_runs; j++) {
							temp2[j] = n_tuples[j];
						}
						n_tuples = temp2;
						n_runs *= 2;
					}

					try {
						temp_files[run_num] = new Heapfile(tmp_file_prefix+run_num);
					} catch (Exception e) {
						throw new SortException(e, "Sort.java: create Heapfile failed");
					}

					// set the last Elem to be the minimum value for the sort field
					if (order.tupleOrder == TupleOrder.Ascending) {
						lastSum = Double.MIN_VALUE;
					} else {
						lastSum = Double.MAX_VALUE;
					}

					// switch the current heap and the other heap
					pnodeSplayPQ tempQ = pcurr_Q;
					pcurr_Q = pother_Q;
					pother_Q = tempQ;
					int tempelems = p_elems_curr_Q;
					p_elems_curr_Q = p_elems_other_Q;
					p_elems_other_Q = tempelems;
				}
			} // end of if (p_elems_curr_Q == 0)
		} // end of while (true)

		// close the last run
		run_num++;

		return run_num;
	}
	/**
	 * Set up for merging the runs. Open an input buffer for each run, and insert
	 * the first element (min) from each run into a heap. <code>delete_min() </code>
	 * will then get the minimum of all runs.
	 * 
	 * @param tuple_size size (in bytes) of each tuple
	 * @param n_R_runs   number of runs
	 * @exception IOException     from lower layers
	 * @exception LowMemException there is not enough memory to sort in two passes
	 *                            (a subclass of SortException).
	 * @exception SortException   something went wrong in the lower layer.
	 * @exception Exception       other exceptions
	 */
	private void setup_for_merge(int tuple_size, int n_R_runs)
			throws IOException, LowMemException, SortException, Exception {
		while(n_R_runs*2>_n_pages) {
			if(_n_pages<6) {
				throw new Exception("Can not sort with less than 6 pages.");
			}
			Heapfile tmp_file = new Heapfile(tmp_file_prefix+n_R_runs);
			
			Scan first = new Scan(temp_files[n_R_runs-2]);
			Scan second = new Scan(temp_files[n_R_runs-1]);
			Tuple f = first.getNext(new RID());
			Tuple s = second.getNext(new RID());
			while(f!=null&&s!=null) {
				f.setHdr(n_cols, _in, str_lens);
				s.setHdr(n_cols, _in, str_lens);
				double sum1 = TupleUtils.getPrefAttrSum(f, _in, n_cols, pref_list, pref_list_len);
				double sum2 = TupleUtils.getPrefAttrSum(s, _in, n_cols, pref_list, pref_list_len);
				switch(order.tupleOrder) {
				case TupleOrder.Descending:
					if(sum2>sum1) {
						tmp_file.insertRecord(s.getTupleByteArray());
						s = second.getNext(new RID());
					}else {
						tmp_file.insertRecord(f.getTupleByteArray());
						f = first.getNext(new RID());						
					}
					break;
				case TupleOrder.Ascending:
					if(sum2<sum1) {
						tmp_file.insertRecord(s.getTupleByteArray());
						s = second.getNext(new RID());
					}else {
						tmp_file.insertRecord(f.getTupleByteArray());
						f = first.getNext(new RID());
					}
					break;
				}
			}
			while(f!=null) {
				f.setHdr(n_cols, _in, str_lens);
				tmp_file.insertRecord(f.getTupleByteArray());
				f = first.getNext(new RID());
			}
			while(s!=null) {
				s.setHdr(n_cols, _in, str_lens);
				tmp_file.insertRecord(s.getTupleByteArray());
				s = second.getNext(new RID());
			}
			first.closescan();
			second.closescan();
			temp_files[n_R_runs-2].deleteFile();
			temp_files[n_R_runs-1].deleteFile();
			temp_files[n_R_runs-2] = tmp_file;
			n_R_runs--;
			Heapfile[] temp1 = new Heapfile[n_R_runs];
			for (int i = 0; i < n_R_runs; i++) {
				temp1[i] = temp_files[i];
			}
			temp_files = temp1;
		}
		int i;
		pnode cur_node; 

		i_buf = new Scan[n_R_runs];
		for (int j = 0; j < n_R_runs; j++)
			i_buf[j] = new Scan(temp_files[j]);

		//Enqueuing first tuple of all the runs into a priority queue to prepare them for sorted access
		for (i = 0; i < n_R_runs; i++) {
			
			cur_node = new pnode();
			cur_node.run_num = i;

			
			Tuple temp_tuple = null;

			

			temp_tuple = i_buf[i].getNext(new RID());

			
			if (temp_tuple != null) {
				try {
					temp_tuple.setHdr(n_cols, _in, str_lens);
				} catch (Exception e) {
					throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
				}
				cur_node.tuple = temp_tuple; 
				try {
					
					Q.enqPref(cur_node);
				} catch (UnknowAttrType e) {
					throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enqPref()");
				} catch (TupleUtilsException e) {
					throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enqPref()");
				}

			}
		}
		return;
	}
	/**
	 * Remove the minimum value among all the runs.
	 * 
	 * @return the minimum tuple removed
	 * @exception IOException   from lower layers
	 * @exception SortException something went wrong in the lower layer.
	 */
	private Tuple delete_min() throws IOException, SortException, Exception {
		pnode cur_node; // needs pq_defs.java
		Tuple new_tuple, old_tuple;

		cur_node = Q.deqPref();
		old_tuple = cur_node.tuple;
		
		
		// we just removed one tuple from one run, now we need to put another
		// tuple of the same run into the queue
		new_tuple = i_buf[cur_node.run_num].getNext(new RID());
		if (new_tuple != null) {
			// run not exhausted
			
			try {
				new_tuple.setHdr(n_cols, _in, str_lens);
			} catch (Exception e) {
				throw new SortException(e, "Sort.java: setHdr() failed");
			}

				cur_node.tuple = new_tuple;
				try {
					Q.enqPref(cur_node);
				} catch (UnknowAttrType e) {
					throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enqPref()");
				} catch (TupleUtilsException e) {
					throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enqPref()");
				}

		}

		return old_tuple;
	}
	
	
	
	/**
	 * Returns the next tuple in sorted order. Note: You need to copy out the
	 * content of the tuple, otherwise it will be overwritten by the next
	 * <code>get_next()</code> call.
	 * 
	 * @return the next tuple, null if all tuples exhausted
	 * @exception IOException     from lower layers
	 * @exception SortException   something went wrong in the lower layer.
	 * @exception JoinsException  from <code>generate_runs()</code>.
	 * @exception UnknowAttrType  attribute type unknown
	 * @exception LowMemException memory low exception
	 * @exception Exception       other exceptions
	 */
	@Override
	public Tuple get_next()
			throws IOException, SortException, UnknowAttrType, LowMemException, JoinsException, Exception {
		

		if (Q.empty()) {
			// no more tuples availble
			return null;
		}

		output_tuple = delete_min();
		if (output_tuple != null) {
			op_buf.tupleCopy(output_tuple);
			return op_buf;
		} else
			return null;
	}

	/**
	 * Cleaning up, including releasing buffer pages from the buffer pool and
	 * removing temporary files from the database.
	 * 
	 * @exception IOException   from lower layers
	 * @exception SortException something went wrong in the lower layer.
	 */
	@Override
	public void close() throws SortException, IOException {
		// clean up
		if (!closeFlag) {

			try {
				_am.close();
			} catch (Exception e) {
				throw new SortException(e, "Sort.java: error in closing iterator.");
			}

			
			for(int i=0;i<i_buf.length;i++) {
				i_buf[i].closescan();
			}
			for (int i = 0; i < temp_files.length; i++) {
				if (temp_files[i] != null) {
					try {
						temp_files[i].deleteFile();
					} catch (Exception e) {
						throw new SortException(e, "Sort.java: Heapfile error");
					}
					temp_files[i] = null;
				}
			}
			closeFlag = true;
		}
	}
	
}
