package iterator;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import heap.*;
import index.IndexException;

import java.io.IOException;

/**
 * This file contains an implementation of the nested loops skyline
 * algorithm.
 */

public class NestedLoopsSky extends Iterator {
  private final AttrType[] _in1;
  private final int in1_len;
  private final short[] t1_str_sizes;
  private final FileScan outerLoopScanner;
  private final int[] pref_list;
  private final int pref_list_length;
  private final Heapfile hf, tempFile;
  private final AttrType[] attrTypesRid;
  private final int tuple_size, maxTuplesInBuffer, maxTuplesInPage;
  private final RID temp_rid;
  private boolean isDone, isDominated, skipInnerLoop;
  private Tuple placeholder;
  private int outerIndex;
  private byte[][] buffer1;

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
   * @throws NestedLoopException exception from this class
   */
  public NestedLoopsSky(AttrType[] in1,
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
    outerLoopScanner = (FileScan) am1;
    this.t1_str_sizes = t1_str_sizes;
    isDone = false;                       //Indicates the completion of skyline computation
    isDominated = false;
    this.pref_list = pref_list;
    this.pref_list_length = pref_list_length;
    skipInnerLoop = false;                //Skip inner loop computation if the outer loop element is already dominated
    outerIndex = 0;
    temp_rid = new RID();

    //Check if there is at least one buffer page for computation
    n_pages-=6; //reserving 6 pages for 3 scans (inner+outer+temp) and 2 pages for creating new heap files (these are reused)
    if (n_pages < 1) {
      System.out.println("NestedLoopsSky: Insufficient buffer pages");
      System.out.println("NestedLoopsSky: At least 7 pages are required for nestedLoopSkyline");
      throw new NestedLoopException("Not enough buffer pages to compute the skyline");
    }

    //Until we have <= 10 pages we use them all for the buffer, if more than that, we use 10 + the third of whatever is extra
    if (n_pages>10){
      if((n_pages-10)/3 >= 1)
        n_pages = 10 + (n_pages-10)/3;
    }

    //Allocating 2D byte array = size of pages left for use (after reserving 6 pages for other tasks)
    buffer1 = new byte[n_pages][GlobalConst.MINIBASE_PAGESIZE];
    //we are using 1 bit to store the status(dominated?) of one tuple
    //therefore, multiplying bytes with 8
    maxTuplesInPage = GlobalConst.MINIBASE_PAGESIZE * 8;
    maxTuplesInBuffer = n_pages * maxTuplesInPage;

    try {
      hf = new Heapfile(relationName);      //input data file
    } catch (Exception e) {
      throw new NestedLoopException(e, "Create new HeapFile failed.");
    }

    try {
      tempFile = new Heapfile(null);       //Create a temporary heap file on the disk in case the buffer gets full
    } catch (Exception e) {
      throw new NestedLoopException(e, "Heap file error");
    }

    placeholder = new Tuple();                   //Dummy tuple for storing tuple index values in the temp heap file
    attrTypesRid = new AttrType[1];
    attrTypesRid[0] = new AttrType(AttrType.attrInteger);     //For tuple index

    try {
      placeholder.setHdr((short) 1, attrTypesRid, this.t1_str_sizes);
    } catch (Exception e) {
      throw new NestedLoopException(e, "t.setHdr() failed");
    }

    tuple_size = placeholder.size();
    System.out.println("NestedLoopsSky operator initialized");
  }

  /**
   * This function will compute the next skyline tuple based on given preferences
   *
   * @return A skyline tuple is returned
   * @throws NestedLoopException exception from this class
   * @throws Exception           other exceptions
   */
  public Tuple get_next()
          throws NestedLoopException, Exception {
    if (isDone)             //Stop computation
      return null;

    do {                    //Start the outer loop
      skipInnerLoop = false;
      Tuple outerTuple;

      try {
        outerTuple = outerLoopScanner.get_next(temp_rid);
      } catch (Exception e) {
        throw new NestedLoopException(e, "reading disk file failed");
      }

      if (outerTuple == null) {               //When outer loop is done
        isDone = true;
        return null;
      }

      if (outerIndex < maxTuplesInBuffer) {    //If outerIndex is present in the buffer
        skipInnerLoop = isPresentInBuffer(outerIndex);   //If outerIndex is marked as dominated in the buffer
      } else {
        skipInnerLoop = isPresentOnDisk(outerIndex);     //If outerIndex is present in the disk
      }

      //If outer tuple is not marked as dominated, go ahead and begin inner loop
      if (!skipInnerLoop) {
        isDominated = false;
        boolean begin_inner = false;

        Scan innerLoopScanner;
        try {
          innerLoopScanner = hf.openScan();
        } catch (Exception e) {
          throw new NestedLoopException(e, "openScan failed");
        }
        int innerIndex = 0;
        Tuple innerTuple;
        while ((innerTuple = innerLoopScanner.getNext(temp_rid)) != null) {
          innerTuple.setHdr((short) in1_len, _in1, t1_str_sizes);
          if (begin_inner) {
            //If inner dominates outer, then outer can never be a skyline attribute -- break! Go to next outer tuple
            if (TupleUtils.Dominates(innerTuple, _in1, outerTuple, _in1,
                    Short.parseShort(in1_len + ""), t1_str_sizes,
                    pref_list, pref_list_length)) {
              isDominated = true;
              break;
            } else if (TupleUtils.Dominates(outerTuple, _in1, innerTuple, _in1, Short.parseShort(in1_len + ""), t1_str_sizes,
                    pref_list, pref_list_length)) {
              //if outer dominates inner, then inner tuple can never be a skyline attribute
              //-- mark it for future reference. Add it to the buffer. We will skip this tuple in the outer loop.
              if (innerIndex < maxTuplesInBuffer) {
                  setInBuffer(innerIndex);
              } else if (!isPresentOnDisk(innerIndex)) {
                  Tuple temp1 = new Tuple(tuple_size);
                  temp1.setHdr((short) 1, attrTypesRid, t1_str_sizes);
                  temp1.setIntFld(1, innerIndex);
                  tempFile.insertRecord(temp1.getTupleByteArray());
              }
            }
          }
          if (outerIndex == innerIndex) {
            //When position of innerScanner equals outerScanner, then start inner loop computation.
            //Reason - All the other tuples are already compared and the results are marked
            begin_inner = true;
          }
          innerIndex++;
        }
        innerLoopScanner.closescan();
        if (!isDominated) {
          outerIndex++;
          //If outer tuple is not dominated by any of the other tuples in the data, then it is a skyline tuple
          return outerTuple;
        }
      }
      outerIndex++;
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

      //Delete temporary heap file
      try {
        tempFile.deleteFile();
      } catch (Exception e) {
        throw new IOException("Heap file error", e);
      }

      closeFlag = true;
    }
  }

  private boolean isPresentInBuffer(int index) {
    //calculate the page number
    int page_no = index / (maxTuplesInPage);
    //calculate the bit number in a page
    int temp = index % (maxTuplesInPage);
    //calculate the byte number in a page
    int byte_no = temp / 8;
    //calculate the bit number in a byte
    temp = temp % 8;

    byte b = buffer1[page_no][byte_no];
    //check if the bit is set?
    return ((b >> temp) & 1) == 1;
  }

  private void setInBuffer(int index) {
    //calculate the page number
    int page_no = index / (maxTuplesInPage);
    //calculate the bit number in a page
    int temp = index % (maxTuplesInPage);
    //calculate the byte number in a page
    int byte_no = temp / 8;
    //calculate the bit number in a byte
    temp = temp % 8;

    //set the bit
    buffer1[page_no][byte_no] |= 1 << temp;
  }

  private boolean isPresentOnDisk(int index) throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException {
    boolean result = false;
    Scan scd1 = tempFile.openScan();

    //Iterating over temp heap file (dominated tuples)
    while ((placeholder = scd1.getNext(temp_rid)) != null) {
      placeholder.setHdr((short) 1, attrTypesRid, t1_str_sizes);

      if (index == placeholder.getIntFld(1)) {
        result = true;
        break;
      }
    }
    scd1.closescan();
    return result;
  }
}
