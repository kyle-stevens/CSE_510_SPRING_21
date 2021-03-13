package iterator;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;
import java.util.Arrays;

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

  private boolean isDone, isDominated, skipInnerLoop;
  private final Heapfile hf, tempFile;
  private final AttrType[] attrTypesRid;
  private Tuple placeholder;
  private int outerIndex, bufferIndex;
  private final int tuple_size;
  private final RID temp_rid;
  private final int[] buffer;

  /**
   *  constructor
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
    bufferIndex = 0;
    temp_rid = new RID();

    //Check if there is at least one buffer page for computation
    if(n_pages < 1){
      System.out.println("NestedLoopsSky: Insufficient buffer pages");
      throw new NestedLoopException("Not enough buffer pages assigned to carry out the operation");
    }

    //calculate how many ints can be stored in the buffer
    //total size available = pages x pageSize ( in bytes)
    //here 12 bytes is the array header overhead
    //and 4 bytes because each int uses 4 bytes of memory
    int arraySize = ((n_pages * GlobalConst.MINIBASE_PAGESIZE)-12)/4;
    buffer = new int[arraySize];                                      //Initialize the buffer
    Arrays.fill(buffer, -1);

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
    System.out.println("Initializing NestedLoopsSky operator");
    System.out.println("Computing skyline using nestedLoops may take a bit longer(a few minutes) " +
            "if no. of buffer pages are very low (single digit)");
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

      if(Arrays.stream(buffer).anyMatch(x -> x == outerIndex)){       //check if the index present in dominated tuples
        skipInnerLoop = true;
      }

      //If outer tuple is not found in buffer and buffer is full,
      //then we will have to check the disk for dominated tuples
      //and make sure that the outer tuple is not found in the buffer already
      if (buffer[buffer.length -1] != -1 && !skipInnerLoop) {
        Scan scd = tempFile.openScan();

        //Iterating over temp heap file (dominated tuples)
        while ((placeholder = scd.getNext(temp_rid)) != null) {
          placeholder.setHdr((short) 1, attrTypesRid, t1_str_sizes);

          //outer tuple is dominated?
          if (outerIndex == placeholder.getIntFld(1)) {
            skipInnerLoop = true;
            break;
          }
        }
        scd.closescan();
      }

      //If outer tuple is not dominated, go ahead and begin inner loop
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
              boolean alreadyPresent = false;
              //Check if the dominated tuple is already present in the buffer
              for(int i: buffer){
                if (i == innerIndex) {
                  alreadyPresent = true;
                  break;
                }
              }

              //If dominated tuple is not present in the buffer then check if it is present in the temp file on disk
              if (buffer[buffer.length -1] != -1 && !alreadyPresent) {
                Scan scd1 = tempFile.openScan();

                //Iterating over temp heap file (dominated tuples)
                while ((placeholder = scd1.getNext(temp_rid)) != null) {
                  placeholder.setHdr((short) 1, attrTypesRid, t1_str_sizes);

                  if (innerIndex == placeholder.getIntFld(1)) {
                    alreadyPresent = true;
                    break;
                  }
                }
                scd1.closescan();
              }

              //If dominated tuple doesn't exist in the set, add it!
              if(!alreadyPresent){
                if(bufferIndex==buffer.length){                     //if buffer full
                  Tuple temp1 = new Tuple(tuple_size);
                  temp1.setHdr((short) 1, attrTypesRid, t1_str_sizes);
                  temp1.setIntFld(1, innerIndex);
                  tempFile.insertRecord(temp1.getTupleByteArray());
                }else {                                             //if buffer not full
                  buffer[bufferIndex] = innerIndex;
                  bufferIndex++;
                }
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
}
