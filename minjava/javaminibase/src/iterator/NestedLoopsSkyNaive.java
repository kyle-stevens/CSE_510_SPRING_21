package iterator;

import global.AttrType;
import global.RID;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;

/**
 * This file contains an implementation of the nested loops skyline
 * algorithm without using buffer pages to store dominated tuples.
 * This is a naive approach, like cross join.
 */

public class NestedLoopsSkyNaive extends Iterator {
  private final AttrType[] _in1;
  private final int in1_len;
  private final short[] t1_str_sizes;
  private final Iterator outerLoopScanner;
  private final int[] pref_list;
  private final int pref_list_length;

  private boolean isDone, isDominated;
  private Tuple innerTuple;
  private final Heapfile hf;

  /**
   * constructor
   *
   * @param in1              Array containing field types of R.
   * @param len_in1          # of columns in R.
   * @param t1_str_sizes     shows the length of the string fields.
   * @param am1              access method for input data
   * @param relationName     access heap file for input data
   * @param pref_list        list of preference attributes
   * @param pref_list_length length of list of preference attributes
   * @param n_pages          number of buffer pages to be allocated for this iterator
   * @throws NestedLoopException       exception from this class
   */
  public NestedLoopsSkyNaive(AttrType[] in1,
                             int len_in1,
                             short[] t1_str_sizes,
                             Iterator am1,
                             String relationName,
                             int[] pref_list,
                             int pref_list_length,
                             int n_pages
  ) throws NestedLoopException {
    _in1 = new AttrType[in1.length];
    System.arraycopy(in1, 0, _in1, 0, in1.length);
    in1_len = len_in1;
    outerLoopScanner = am1;
    this.t1_str_sizes = t1_str_sizes;
    innerTuple = new Tuple();
    isDone = false;                           //Indicates the completion of skyline computation
    isDominated = false;
    this.pref_list = pref_list;
    this.pref_list_length = pref_list_length;

    try {
      hf = new Heapfile(relationName);        //input data file
    } catch (Exception e) {
      throw new NestedLoopException(e, "Create new heapfile failed.");
    }
  }

  /**
   * @return The skyline tuple is returned
   * @throws NestedLoopException exception from this class
   * @throws Exception           other exceptions
   */
  public Tuple get_next()
          throws NestedLoopException,
          Exception {
    if (isDone)
      return null;                        //Stop computation

    do {
      Scan innerLoopScanner;
      try {
        innerLoopScanner = hf.openScan();
      } catch (Exception e) {
        throw new NestedLoopException(e, "openScan failed");
      }

      Tuple outerTuple;
      if ((outerTuple = outerLoopScanner.get_next()) == null) {
        isDone = true;                              //When outer loop is done
        if (innerLoopScanner != null) {
          innerLoopScanner.closescan();
        }
        return null;
      }
      isDominated = false;
      RID rid = new RID();

      //Inner loop
      while ((innerTuple = innerLoopScanner.getNext(rid)) != null) {
        innerTuple.setHdr((short) in1_len, _in1, t1_str_sizes);
        //If both loop read the same tuple
        if (TupleUtils.Equal(outerTuple, innerTuple, _in1, in1_len)) {
          continue;
        }
        //If inner dominates outer, then outer can never be a skyline attribute -- break! Go to next outer tuple
        if (TupleUtils.Dominates(innerTuple, _in1, outerTuple, _in1,
                Short.parseShort(in1_len + ""), t1_str_sizes,
                pref_list, pref_list_length)) {
          isDominated = true;
          break;
        }
      }

      if (!isDominated) {
        innerLoopScanner.closescan();
        return outerTuple;
      }
    } while (true);
  }

  /**
   * implement the abstract method close() from super class Iterator
   * to finish cleaning up
   *
   * @throws IOException    I/O error from lower layers
   * @throws IndexException index access error
   */
  public void close() throws IOException, IndexException {
    if (!closeFlag) {
      //Close Scanner
      try {
        outerLoopScanner.close();
      } catch (Exception e) {
        throw new IOException("NestLoopSky.java: error in closing iterator.", e);
      }
      closeFlag = true;
    }
  }
}
