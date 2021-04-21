package iterator;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import heap.*;
import index.IndexException;

import java.io.IOException;

/**
 * This file contains an implementation of the nested loops join
 * algorithm as described in the Shapiro paper.
 * The algorithm is extremely simple:
 * <p>
 * foreach tuple r in R do
 * foreach tuple s in S do
 * if (ri == sj) then add (r, s) to the result.
 */

public class CustomNLJ extends Iterator {
  private AttrType _in1[], _in2[];
  private int in1_len, in2_len;
  private Iterator outer;
  private short t1_str_sizecopy[];
  private short t2_str_sizescopy[];
  private CondExpr OutputFilter[];
  private boolean outerScanDone, completed;
  private Tuple outer_tuple, inner_tuple;
  private Tuple Jtuple;           // Joined tuple
  private FldSpec perm_mat[];
  private int nOutFlds;
  private Heapfile hf;
  private Scan inner;

  private NRABuffer keyBuffer;
  private AttrType[] keyAttrs;
  private short[] key_str_lens;
  private int outerJoinField, innerJoinField;
  private boolean loopInner, outerJoin;
  private Heapfile keyDataFile;


  /**
   * constructor
   * Initialize the two relations which are joined, including relation type,
   *
   * @param in1          Array containing field types of R.
   * @param len_in1      # of columns in R.
   * @param t1_str_sizes shows the length of the string fields.
   * @param in2          Array containing field types of S
   * @param len_in2      # of columns in S
   * @param t2_str_sizes shows the length of the string fields.
   * @param am1          access method for left i/p to join
   * @param relationName access hfapfile for right i/p to join
   * @param outFilter    select expressions
   * @param proj_list    shows what input fields go where in the output tuple
   * @param n_out_flds   number of outer relation fileds
   * @throws IOException         some I/O fault
   * @throws NestedLoopException exception from this class
   */
  public CustomNLJ(AttrType in1[],
                   int len_in1,
                   short t1_str_sizes[],
                   AttrType in2[],
                   int len_in2,
                   short t2_str_sizes[],
                   Iterator am1,
                   String relationName,
                   CondExpr outFilter[],
                   FldSpec proj_list[],
                   int n_out_flds,
                   NRABuffer keyBuffer,
                   AttrType[] keyAttrs,
                   short[] key_str_lens,
                   int outerJoinField,
                   int innerJoinField,
                   Heapfile keyDataFile
  ) throws IOException, NestedLoopException {

    _in1 = new AttrType[in1.length];
    _in2 = new AttrType[in2.length];
    System.arraycopy(in1, 0, _in1, 0, in1.length);
    System.arraycopy(in2, 0, _in2, 0, in2.length);
    in1_len = len_in1;
    in2_len = len_in2;

    this.keyBuffer = keyBuffer;
    this.keyAttrs = keyAttrs;
    this.key_str_lens = key_str_lens;
    this.outerJoinField = outerJoinField;
    this.innerJoinField = innerJoinField;
    loopInner = false;
    outerJoin = false;
    completed = false;
    this.keyDataFile = keyDataFile;

    outer = am1;
    t1_str_sizecopy = t1_str_sizes;
    t2_str_sizescopy = t2_str_sizes;
    inner_tuple = new Tuple();
    Jtuple = new Tuple();
    OutputFilter = outFilter;

    inner = null;
    outerScanDone = false;

    AttrType[] Jtypes = new AttrType[n_out_flds];
    short[] t_size;

    perm_mat = proj_list;
    nOutFlds = n_out_flds;
    try {
      t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
              in1, len_in1, in2, len_in2,
              t1_str_sizes, t2_str_sizes,
              proj_list, nOutFlds);
    } catch (TupleUtilsException e) {
      throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
    }

    try {
      hf = new Heapfile(relationName);
    } catch (Exception e) {
      throw new NestedLoopException(e, "Create new heapfile failed.");
    }
  }

  /**
   * @return The joined tuple is returned
   * @throws IOException               I/O errors
   * @throws JoinsException            some join exception
   * @throws IndexException            exception from super class
   * @throws InvalidTupleSizeException invalid tuple size
   * @throws InvalidTypeException      tuple type not valid
   * @throws PageNotReadException      exception from lower layer
   * @throws TupleUtilsException       exception from using tuple utilities
   * @throws PredEvalException         exception from PredEval class
   * @throws SortException             sort exception
   * @throws LowMemException           memory error
   * @throws UnknowAttrType            attribute type unknown
   * @throws UnknownKeyTypeException   key type unknown
   * @throws Exception                 other exceptions
   */
  public Tuple get_next()
          throws IOException,
          JoinsException,
          IndexException,
          InvalidTupleSizeException,
          InvalidTypeException,
          PageNotReadException,
          TupleUtilsException,
          PredEvalException,
          SortException,
          LowMemException,
          UnknowAttrType,
          UnknownKeyTypeException,
          Exception {

    if (completed)
      return null;

    if (!outerScanDone) {
      while (true) {

        if (inner != null) {
          inner.closescan();
          inner = null;
        }

        try {
          inner = hf.openScan();
        } catch (Exception e) {
          throw new NestedLoopException(e, "openScan failed");
        }

        if ((outer_tuple = outer.get_next()) == null) {
          outerScanDone = true;
          if (inner != null) {
            inner.closescan();
            inner = null;
          }
          outer.close();
          outer = null;
          break;
        }

        Tuple temp;
        keyBuffer.reset_read();
        while ((temp = keyBuffer.Get()) != null) {
          temp.setHdr((short) keyAttrs.length, keyAttrs, key_str_lens);
          if (TupleUtils.CompareTupleWithTuple(_in1[outerJoinField - 1], outer_tuple,
                  outerJoinField, temp, 1) == 0) {
            if (temp.getIntFld(2) == 3)
              loopInner = true;
            else
              outerJoin = true;
            break;
          }
        }

        if (!loopInner && keyBuffer.get_buf_status() && !outerJoin) {
          Scan scd = keyDataFile.openScan();
          Tuple keyTuple;
          RID keyTupleRid = new RID();

          //Iterating over heap file
          while ((keyTuple = scd.getNext(keyTupleRid)) != null) {
            keyTuple.setHdr((short) keyAttrs.length, keyAttrs, key_str_lens);
            if (TupleUtils.CompareTupleWithTuple(_in1[outerJoinField - 1], outer_tuple,
                    outerJoinField, keyTuple, 1) == 0) {
              if (keyTuple.getIntFld(2) == 3)
                loopInner = true;
              else
                outerJoin = true;
              break;
            }
          }
          scd.closescan();
        }

        if (!loopInner && !outerJoin) {
          continue;
        }

        if (loopInner) {
          RID rid = new RID();
          while ((inner_tuple = inner.getNext(rid)) != null) {
            inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
            if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2)) {
              // Apply a projection on the outer and inner tuples.
              Projection.Join(outer_tuple, _in1,
                      inner_tuple, _in2,
                      Jtuple, perm_mat, nOutFlds);
              loopInner = false;
              return Jtuple;
            }
          }
        }

        if (outerJoin) {
          Tuple dummyInner = new Tuple();
          dummyInner.setHdr((short) in2_len, _in2, t2_str_sizescopy);
          for (int i = 0; i < in2_len; i++) {
            switch (_in2[i].attrType) {
              case AttrType.attrInteger:
                dummyInner.setIntFld(i + 1, 0);
                break;
              case AttrType.attrReal:
                dummyInner.setFloFld(i + 1, 0);
                break;
              case AttrType.attrString:
                dummyInner.setStrFld(i + 1, "none");
                break;
            }
          }
          Projection.Join(outer_tuple, _in1,
                  dummyInner, _in2,
                  Jtuple, perm_mat, nOutFlds);
          outerJoin = false;
          return Jtuple;
        }
      }
    }

    if (inner == null) {
      try {
        inner = hf.openScan();
      } catch (Exception e) {
        throw new NestedLoopException(e, "openScan failed");
      }
    }

    boolean innerJoin = false;

    while (true) {
      RID rid = new RID();
      inner_tuple = inner.getNext(rid);

      if (inner_tuple == null) {
        completed = true;
        inner.closescan();
        inner = null;
        return null;
      }

      inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);

      Tuple temp;
      keyBuffer.reset_read();
      while ((temp = keyBuffer.Get()) != null) {
        temp.setHdr((short) keyAttrs.length, keyAttrs, key_str_lens);
        if (TupleUtils.CompareTupleWithTuple(_in2[innerJoinField - 1], inner_tuple,
                innerJoinField, temp, 1) == 0 && temp.getIntFld(2) == 1) {
          innerJoin = true;
          break;
        }
      }

      if (!innerJoin && keyBuffer.get_buf_status()) {
        Scan scd = keyDataFile.openScan();
        Tuple keyTuple;
        RID keyTupleRid = new RID();

        //Iterating over heap file
        while ((keyTuple = scd.getNext(keyTupleRid)) != null) {
          keyTuple.setHdr((short) keyAttrs.length, keyAttrs, key_str_lens);
          if (TupleUtils.CompareTupleWithTuple(_in2[innerJoinField - 1], inner_tuple,
                  innerJoinField, keyTuple, 1) == 0 && keyTuple.getIntFld(2) == 1) {
            innerJoin = true;
            break;
          }
        }
        scd.closescan();
      }

      if (!innerJoin) {
        continue;
      }

      Tuple dummyOuter = new Tuple();
      dummyOuter.setHdr((short) in1_len, _in1, t1_str_sizecopy);
      for (int i = 0; i < in1_len; i++) {
        switch (_in1[i].attrType) {
          case AttrType.attrInteger:
            dummyOuter.setIntFld(i + 1, 0);
            break;
          case AttrType.attrReal:
            dummyOuter.setFloFld(i + 1, 0);
            break;
          case AttrType.attrString:
            dummyOuter.setStrFld(i + 1, "none");
            break;
        }
      }
      Projection.Join(dummyOuter, _in1,
              inner_tuple, _in2,
              Jtuple, perm_mat, nOutFlds);
      return Jtuple;
    }
  }

  /**
   * implement the abstract method close() from super class Iterator
   * to finish cleaning up
   *
   * @throws IOException    I/O error from lower layers
   * @throws JoinsException join error from lower layers
   * @throws IndexException index access error
   */
  public void close() throws JoinsException, IOException, IndexException, SortException {
    if(outer != null) {
      outer.close();
    }

    if(inner != null) {
      inner.closescan();
    }
  }
}
