package iterator;

import btree.IntegerKey;
import btree.KeyClass;
import btree.RealKey;
import btree.StringKey;
import global.*;
import heap.*;
import index.ClusteredBtreeIndexScan;
import index.IndexException;

import java.io.IOException;

public class TopKNRAJoin extends Iterator{

  private final AttrType[] outer_in;
  private final short[] outer_str_lens;

  private final AttrType[] inner_in;
  private final short[] inner_str_lens;

  private final int outer_join_attr;
  private final int inner_join_attr;

  private final int outer_merge_attr;
  private final int inner_merge_attr;

  private final int n_pages;
  private final int k;

  private final ClusteredBtreeIndexScan innerScan;
  private final ClusteredBtreeIndexScan outerScan;

  private final NRABuffer tupleMetaData;
  private NRABuffer joinKeyData;
  private PageId[] bufferPIDs;
  private final AttrType[] metaDataAttrTypes;

  private final Heapfile metaDataFile;
  private final String metaDataFileName;
  private short[] meta_str_lens;
  private final int metaTupleSize;

  private final Heapfile joinKeyFile;
  private final AttrType[] joinKeyAttrTypes;
  private short[] join_key_str_lens;
  private final int joinKeyTupleSize;

  private final Heapfile outerCandidateFile;
  private final String outerCandidateFileName;
  private final Heapfile innerCandidateFile;
  private final String innerCandidateFileName;

  private CustomNLJ nlj;

  public TopKNRAJoin(
          AttrType[] in1,
          short[] t1_str_sizes,
          int joinAttr1,
          int mergeAttr1,
          AttrType[] in2,
          short[] t2_str_sizes,
          int joinAttr2,
          int mergeAttr2,
          String relationName1,
          String relationName2,
          int k,
          int n_pages
  ) throws Exception {
    this.outer_in = in1;
    this.inner_in = in2;
    this.outer_str_lens = t1_str_sizes;
    this.inner_str_lens = t2_str_sizes;
    this.outer_join_attr = joinAttr1;
    this.inner_join_attr = joinAttr2;
    this.outer_merge_attr = mergeAttr1;
    this.inner_merge_attr = mergeAttr2;
    this.n_pages = n_pages;
    this.k = k;
    this.metaDataFileName = "NRAMetadata";
    String joinKeyFileName = "NRAJoinKeyData";
    this.outerCandidateFileName = "NRAOuterCandidates";
    this.innerCandidateFileName = "NRAInnerCandidates";

    String outerIndexFile = "clst_bt_" + relationName1 + "_" + mergeAttr1;
    String innerIndexFile = "clst_bt_" + relationName2 + "_" + mergeAttr2;

    innerScan = new ClusteredBtreeIndexScan(innerIndexFile, inner_in, inner_str_lens, null, inner_merge_attr, true);
    outerScan = new ClusteredBtreeIndexScan(outerIndexFile, outer_in, outer_str_lens, null, inner_merge_attr, true);

    try {
      metaDataFile = new Heapfile(metaDataFileName);
    } catch (Exception e) {
      throw new NRAException(e, "Heap file error");
    }

    Tuple temp = new Tuple();                   //Dummy tuple to initialize buffer
    metaDataAttrTypes = new AttrType[4];
    metaDataAttrTypes[0] = new AttrType(outer_in[outer_join_attr - 1].attrType);     //For key
    metaDataAttrTypes[1] = new AttrType(AttrType.attrReal);     //For LowerBound
    metaDataAttrTypes[2] = new AttrType(AttrType.attrReal);     //For UpperBound
    metaDataAttrTypes[3] = new AttrType(AttrType.attrInteger);     //For source

    if(metaDataAttrTypes[0].attrType == AttrType.attrString) {
      meta_str_lens = new short[1];
      meta_str_lens[0] = GlobalConst.MAX_STR_LEN + 2;
    }

    try {
      temp.setHdr((short) metaDataAttrTypes.length, metaDataAttrTypes, this.meta_str_lens);
    } catch (Exception e) {
      throw new NRAException(e, "t.setHdr() failed");
    }

    metaTupleSize = temp.size();

    try {
      joinKeyFile = new Heapfile(joinKeyFileName);
    } catch (Exception e) {
      throw new NRAException(e, "Heap file error");
    }

    Tuple tempJoinKey = new Tuple();
    joinKeyAttrTypes = new AttrType[2];
    joinKeyAttrTypes[0] = new AttrType(outer_in[outer_join_attr - 1].attrType);
    joinKeyAttrTypes[1] = new AttrType(AttrType.attrInteger);
    if(joinKeyAttrTypes[0].attrType == AttrType.attrString) {
      join_key_str_lens = new short[1];
      join_key_str_lens[0] = GlobalConst.MAX_STR_LEN + 2;
    }

    try {
      tempJoinKey.setHdr((short) joinKeyAttrTypes.length, joinKeyAttrTypes, this.join_key_str_lens);
    } catch (Exception e) {
      throw new NRAException(e, "t.setHdr() failed");
    }

    joinKeyTupleSize = tempJoinKey.size();

    try {
      outerCandidateFile = new Heapfile(outerCandidateFileName);
    } catch (Exception e) {
      throw new NRAException(e, "Heap file error");
    }

    try {
      innerCandidateFile = new Heapfile(innerCandidateFileName);
    } catch (Exception e) {
      throw new NRAException(e, "Heap file error");
    }

    tupleMetaData = new NRABuffer();
    this.bufferPIDs = new PageId[n_pages];
    byte[][] buffer = new byte[n_pages][];

    try {
      get_buffer_pages(n_pages, bufferPIDs, buffer);
    } catch (Exception e) {
      throw new NRAException(e, "BUFMgr error");
    }

    tupleMetaData.init(buffer, n_pages, metaTupleSize, metaDataFile, false);   //Initialize the buffer

    computeTopK();
    createTopKBuffer();
//    debug();
    join_topK();
  }

  private void debug() throws InvalidTupleSizeException, IOException, InvalidTypeException {
    Scan sc = outerCandidateFile.openScan();
    Tuple outer;
    RID o = new RID();
    System.out.println("Printing outer candidate file");
    while ((outer = sc.getNext(o)) != null){
      outer.setHdr((short) outer_in.length, outer_in, outer_str_lens);
      outer.print(outer_in);
    }
    sc.closescan();

    Scan sc1 = innerCandidateFile.openScan();
    Tuple inner;
    RID i = new RID();
    System.out.println("Printing inner candidate file");
    while ((inner = sc1.getNext(i)) != null){
      inner.setHdr((short) inner_in.length, inner_in, inner_str_lens);
      inner.print(inner_in);
    }
    sc1.closescan();
  }

  private void computeTopK() throws Exception {
    int bufferSize = 0;
    int passingCandidates;
    while(true) {
      float threshold, innerMergeValue=0, outerMergeValue=0;
      Tuple inner = innerScan.get_reversed_next();
      Tuple outer = outerScan.get_reversed_next();
      passingCandidates = 0;

      if(inner == null && outer == null)
        break;

      if(inner != null && outer != null) {
        inner.setHdr((short) inner_in.length, inner_in, inner_str_lens);
        outer.setHdr((short) outer_in.length, outer_in, outer_str_lens);
        KeyClass kc_outer = getJoinKey(outer, true);
        KeyClass kc_inner = getJoinKey(inner, false);
        innerMergeValue = getMergeAttr(inner, false);
        outerMergeValue = getMergeAttr(outer, true);
        threshold = innerMergeValue + outerMergeValue;
        boolean innerMatched = false, outerMatched = false;

        Tuple temp;
        tupleMetaData.reset_read();
        while ((temp = tupleMetaData.Get()) != null) {
          temp.setHdr((short) metaDataAttrTypes.length, metaDataAttrTypes, meta_str_lens);
          int source = temp.getIntFld(4);
          KeyClass key = getJoinKeyFromMetaData(temp);
          float lb = temp.getFloFld(2);

          if(source == 1) {
            Tuple temp1;
            if(keyEqual(key, kc_outer)) {
              temp1 = getUpdatedMetaTuple(temp, lb + outerMergeValue, lb + outerMergeValue,
                      3, false);
              outerCandidateFile.insertRecord(outer.returnTupleByteArray());
            } else {
              temp1 = getUpdatedMetaTuple(temp, 0, lb + outerMergeValue, 0, true);
            }
            if(temp1.getFloFld(2) >= threshold) {
              passingCandidates++;
            }
            tupleMetaData.updateCurrent(temp1);
          } else if(source == 2) {
            Tuple temp1;
            if(keyEqual(key, kc_inner)) {
              temp1 = getUpdatedMetaTuple(temp, lb + innerMergeValue, lb + innerMergeValue,
                      3, false);
              innerCandidateFile.insertRecord(inner.returnTupleByteArray());
            } else {
              temp1 = getUpdatedMetaTuple(temp, 0, lb + innerMergeValue,
                      0, true);
            }
            if(temp1.getFloFld(2) >= threshold) {
              passingCandidates++;
            }
            tupleMetaData.updateCurrent(temp1);
          } else if( lb >= threshold) {
             passingCandidates++;
          }

          if(keyEqual(key, kc_inner))
            innerMatched = true;
          if(keyEqual(key, kc_outer))
            outerMatched = true;
        }

        if (tupleMetaData.get_buf_status()) {
          Scan scd = metaDataFile.openScan();
          Tuple metaTuple;
          RID metaTupleRid = new RID();

          //Iterating over heap file
          while ((metaTuple = scd.getNext(metaTupleRid)) != null) {
            metaTuple.setHdr((short) metaDataAttrTypes.length, metaDataAttrTypes, meta_str_lens);
            int source = metaTuple.getIntFld(4);
            KeyClass key = getJoinKeyFromMetaData(metaTuple);
            float lb = metaTuple.getFloFld(2);

            if(source == 1) {
              Tuple temp1;
              if(keyEqual(key, kc_outer)) {
                temp1 = getUpdatedMetaTuple(metaTuple, lb + outerMergeValue, lb + outerMergeValue,
                        3, false);
                outerCandidateFile.insertRecord(outer.returnTupleByteArray());
              } else {
                temp1 = getUpdatedMetaTuple(metaTuple, 0, lb + outerMergeValue, 0, true);
              }
              if(temp1.getFloFld(2) >= threshold) {
                passingCandidates++;
              }
              metaDataFile.updateRecord(metaTupleRid, temp1);
            } else if(source == 2) {
              Tuple temp1;
              if(keyEqual(key, kc_inner)) {
                temp1 = getUpdatedMetaTuple(metaTuple, lb + innerMergeValue, lb + innerMergeValue,
                        3, false);
                innerCandidateFile.insertRecord(inner.returnTupleByteArray());
              } else {
                temp1 = getUpdatedMetaTuple(metaTuple, 0, lb + innerMergeValue,
                        0, true);
              }
              if(temp1.getFloFld(2) >= threshold) {
                passingCandidates++;
              }
              metaDataFile.updateRecord(metaTupleRid, temp1);
            } else if (lb >= threshold){
              passingCandidates++;
            }

            if(keyEqual(key, kc_inner))
              innerMatched = true;
            if(keyEqual(key, kc_outer))
              outerMatched = true;
          }
          scd.closescan();
        }

        if(keyEqual(kc_inner,kc_outer)) {
          if(!innerMatched) {
            float bound = innerMergeValue + outerMergeValue;
            tupleMetaData.insert(newMetaTuple(kc_inner, bound, bound, 3), tupleMetaData.get_buf_status());
            bufferSize++;
            if(bound >= threshold) {
              passingCandidates++;
            }
            outerCandidateFile.insertRecord(outer.returnTupleByteArray());
            innerCandidateFile.insertRecord(inner.returnTupleByteArray());
          }
        } else {
          if(!innerMatched) {
            float upperbound = innerMergeValue + outerMergeValue;
            tupleMetaData.insert(newMetaTuple(kc_inner, innerMergeValue, upperbound, 1),
                    tupleMetaData.get_buf_status());
            bufferSize++;
            if(innerMergeValue >= threshold) {
              passingCandidates ++;
            }
            innerCandidateFile.insertRecord(inner.returnTupleByteArray());
          }
          if(!outerMatched) {
            float upperbound = innerMergeValue + outerMergeValue;
            tupleMetaData.insert(newMetaTuple(kc_outer, outerMergeValue, upperbound, 2),
                    tupleMetaData.get_buf_status());
            bufferSize++;
            if(outerMergeValue >= threshold) {
              passingCandidates ++;
            }
            outerCandidateFile.insertRecord(outer.returnTupleByteArray());
          }
        }
      }

      if(inner == null) {
        outer.setHdr((short) outer_in.length, outer_in, outer_str_lens);
        KeyClass kc_outer = getJoinKey(outer, true);
        outerMergeValue = getMergeAttr(outer, true);
        threshold = outerMergeValue;
        boolean outerMatched = false;

        Tuple temp;
        tupleMetaData.reset_read();
        while ((temp = tupleMetaData.Get()) != null) {
          temp.setHdr((short) metaDataAttrTypes.length, metaDataAttrTypes, meta_str_lens);
          int source = temp.getIntFld(4);
          KeyClass key = getJoinKeyFromMetaData(temp);
          float lb = temp.getFloFld(2);

          if(source == 1) {
            Tuple temp1;
            if(keyEqual(key, kc_outer)) {
              temp1 = getUpdatedMetaTuple(temp, lb + outerMergeValue, lb + outerMergeValue,
                      3, false);
            } else {
              temp1 = getUpdatedMetaTuple(temp, 0, lb + outerMergeValue, 0, true);
            }
            if(temp1.getFloFld(2) >= threshold) {
              passingCandidates++;
            }
            tupleMetaData.updateCurrent(temp1);
          }

          if(keyEqual(key, kc_outer))
            outerMatched = true;
        }

        if (tupleMetaData.get_buf_status()) {
          Scan scd = metaDataFile.openScan();
          Tuple metaTuple;
          RID metaTupleRid = new RID();

          //Iterating over heap file
          while ((metaTuple = scd.getNext(metaTupleRid)) != null) {
            metaTuple.setHdr((short) metaDataAttrTypes.length, metaDataAttrTypes, meta_str_lens);
            int source = metaTuple.getIntFld(4);
            KeyClass key = getJoinKeyFromMetaData(metaTuple);
            float lb = metaTuple.getFloFld(2);
            float ub = metaTuple.getFloFld(3);

            if(source == 1) {
              Tuple temp1;
              if(keyEqual(key, kc_outer)) {
                temp1 = getUpdatedMetaTuple(metaTuple, lb + outerMergeValue, lb + outerMergeValue,
                        3, false);
              } else {
                temp1 = getUpdatedMetaTuple(metaTuple, 0, lb + outerMergeValue, 0, true);
              }
              if(temp1.getFloFld(2) >= threshold) {
                passingCandidates++;
              }
              metaDataFile.updateRecord(metaTupleRid, temp1);
            }

            if(keyEqual(key, kc_outer))
              outerMatched = true;
          }
          scd.closescan();
        }

        if(!outerMatched) {
          tupleMetaData.insert(newMetaTuple(kc_outer, outerMergeValue, outerMergeValue, 2),
                  tupleMetaData.get_buf_status());
          bufferSize++;
          passingCandidates++;
          outerCandidateFile.insertRecord(outer.returnTupleByteArray());
        }
      }

      if(outer == null) {
        inner.setHdr((short) inner_in.length, inner_in, inner_str_lens);
        KeyClass kc_inner = getJoinKey(inner, false);
        innerMergeValue = getMergeAttr(inner, false);
        threshold = innerMergeValue;
        boolean innerMatched = false;

        Tuple temp;
        tupleMetaData.reset_read();
        while ((temp = tupleMetaData.Get()) != null) {
          temp.setHdr((short) metaDataAttrTypes.length, metaDataAttrTypes, meta_str_lens);
          int source = temp.getIntFld(4);
          KeyClass key = getJoinKeyFromMetaData(temp);
          float lb = temp.getFloFld(2);

          if(source == 2) {
            Tuple temp1;
            if(keyEqual(key, kc_inner)) {
              temp1 = getUpdatedMetaTuple(temp, lb + innerMergeValue, lb + innerMergeValue,
                      3, false);
            } else {
              temp1 = getUpdatedMetaTuple(temp, 0, lb + innerMergeValue,
                      0, true);
            }
            if(temp1.getFloFld(2) >= threshold) {
              passingCandidates++;
            }
            tupleMetaData.updateCurrent(temp1);
          }

          if(keyEqual(key, kc_inner))
            innerMatched = true;
        }

        if (tupleMetaData.get_buf_status()) {
          Scan scd = metaDataFile.openScan();
          Tuple metaTuple;
          RID metaTupleRid = new RID();

          //Iterating over heap file
          while ((metaTuple = scd.getNext(metaTupleRid)) != null) {
            metaTuple.setHdr((short) metaDataAttrTypes.length, metaDataAttrTypes, meta_str_lens);
            int source = metaTuple.getIntFld(4);
            KeyClass key = getJoinKeyFromMetaData(metaTuple);
            float lb = metaTuple.getFloFld(2);

            if(source == 2) {
              Tuple temp1;
              if(keyEqual(key, kc_inner)) {
                temp1 = getUpdatedMetaTuple(metaTuple, lb + innerMergeValue, lb + innerMergeValue,
                        3, false);
              } else {
                temp1 = getUpdatedMetaTuple(metaTuple, 0, lb + innerMergeValue,
                        0, true);
              }
              if(temp1.getFloFld(2) >= threshold) {
                passingCandidates++;
              }
              metaDataFile.updateRecord(metaTupleRid, temp1);
            }

            if(keyEqual(key, kc_inner))
              innerMatched = true;
          }
          scd.closescan();
        }

        if(!innerMatched) {
          tupleMetaData.insert(newMetaTuple(kc_inner, innerMergeValue, innerMergeValue, 1),
                  tupleMetaData.get_buf_status());
          bufferSize++;
          passingCandidates++;
          innerCandidateFile.insertRecord(inner.returnTupleByteArray());
        }
      }

      if(passingCandidates >= k) {
        break;
      }
    }

    innerScan.close();
    outerScan.close();

    if(bufferSize > 0)
      tupleMetaData.flush();
    else
      throw new NRAException("NRA Metadata Buffer empty");

    if(passingCandidates < 1) {
      throw new NRAException("Candidate list empty");
    }

    if(passingCandidates < k) {
      System.out.println("Could not find top "+k+" tuples");
      System.out.println("TOPKNRAJoin found top "+passingCandidates+" tuples");
    }
    //Free buffer pages
    try {
      free_buffer_pages(n_pages, bufferPIDs);
    } catch (Exception e) {
      throw new IOException("Buffer Manager error", e);
    }
  }

  private void createTopKBuffer() throws Exception {
    joinKeyData = new NRABuffer();
    this.bufferPIDs = new PageId[n_pages];
    byte[][] buffer = new byte[n_pages][];

    try {
      get_buffer_pages(n_pages, bufferPIDs, buffer);
    } catch (Exception e) {
      throw new NRAException(e, "BUFMgr error");
    }

    joinKeyData.init(buffer, n_pages, joinKeyTupleSize, joinKeyFile, false);   //Initialize the buffer

    FldSpec[] projection = new FldSpec[metaDataAttrTypes.length];
    for (int i = 0; i < metaDataAttrTypes.length; i++) {
      projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
    }

    FileScan tempScan = new FileScan(metaDataFileName, metaDataAttrTypes, meta_str_lens,
            (short) metaDataAttrTypes.length, (short) metaDataAttrTypes.length,
            projection, null);

    Sort sort = new Sort(metaDataAttrTypes, (short) metaDataAttrTypes.length, meta_str_lens, tempScan,
            2, new TupleOrder(TupleOrder.Descending), 4, 200);


    Tuple t;
    for(int i=0; i<k; i++){
      t = sort.get_next();
      if(t == null)
        break;

      Tuple joink = new Tuple(joinKeyTupleSize);
      joink.setHdr((short)joinKeyAttrTypes.length, joinKeyAttrTypes, join_key_str_lens);
      setMetaDataKey(joink,getJoinKeyFromMetaData(t));
      joink.setIntFld(2,t.getIntFld(4));

      joinKeyData.insert(joink, joinKeyData.get_buf_status());

      joink.print(joinKeyAttrTypes);
      System.out.println("Join key = "+getJoinKeyValue(getJoinKeyFromMetaData(t)));
      System.out.println("Source = "+t.getIntFld(4));
      System.out.println("Bounds = "+t.getFloFld(2)+" -- "+t.getFloFld(3));
    }

    sort.close();
    metaDataFile.deleteFile();
  }

  private void join_topK() throws Exception {

    FldSpec[] projection = new FldSpec[outer_in.length];
    for (int i = 0; i < outer_in.length; i++) {
      projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
    }

    FileScan tempScan = new FileScan(outerCandidateFileName, outer_in, outer_str_lens,
            (short) outer_in.length, (short) outer_in.length,
            projection, null);

    CondExpr[] outFilter = new CondExpr[2];

    outFilter[0] = new CondExpr();
    outFilter[0].next = null;
    outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);

    outFilter[1] = null;

    outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
    outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outer_join_attr);

    outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
    outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), inner_join_attr);

    int n_out_flds = inner_in.length + outer_in.length;
    FldSpec[] proj_list = new FldSpec[n_out_flds];

    for (int i = 0; i < outer_in.length; i++) {
      proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
    }
    for (int i = 0; i < inner_in.length; i++) {
      proj_list[i + inner_in.length] = new FldSpec(new RelSpec(RelSpec.innerRel), i + 1);
    }

    nlj = new CustomNLJ(outer_in, outer_in.length, outer_str_lens,
            inner_in, inner_in.length, inner_str_lens,
            tempScan, innerCandidateFileName, outFilter, proj_list, n_out_flds,
            joinKeyData, joinKeyAttrTypes, join_key_str_lens, outer_join_attr, inner_join_attr, joinKeyFile);
  }

  @Override
  public Tuple get_next() throws Exception {
    return nlj.get_next();
  }

  private void setMetaDataKey(Tuple newTupleMeta, KeyClass key) throws FieldNumberOutOfBoundException, IOException {
    switch (metaDataAttrTypes[0].attrType) {
      case AttrType.attrInteger:
        newTupleMeta.setIntFld(1, ((IntegerKey) key).getKey());
        break;
      case AttrType.attrReal:
        newTupleMeta.setFloFld(1, ((RealKey)key).getKey());
        break;
      case AttrType.attrString:
        newTupleMeta.setStrFld(1, ((StringKey) key).getKey());
    }
  }

  private Tuple newMetaTuple(KeyClass key, float lower, float upper, int source) throws InvalidTupleSizeException,
          IOException, InvalidTypeException,
          FieldNumberOutOfBoundException {
    Tuple newTupleMeta = new Tuple(metaTupleSize);
    newTupleMeta.setHdr((short) metaDataAttrTypes.length, metaDataAttrTypes, meta_str_lens);
    setMetaDataKey(newTupleMeta, key);
    newTupleMeta.setFloFld(2, lower);
    newTupleMeta.setFloFld(3, upper);
    newTupleMeta.setIntFld(4, source);
    return newTupleMeta;
  }

  private Tuple getUpdatedMetaTuple(Tuple original, float lower,
                                    float upper, int source, boolean upperOnly) throws InvalidTupleSizeException, IOException,
          InvalidTypeException, FieldNumberOutOfBoundException {
    Tuple temp1 = new Tuple(original);
    temp1.setHdr((short) metaDataAttrTypes.length, metaDataAttrTypes, meta_str_lens);

    if(upperOnly) {
      temp1.setFloFld(3, upper);
      return temp1;
    }

    temp1.setFloFld(2, lower);
    temp1.setFloFld(3, upper);
    temp1.setIntFld(4, source);
    return temp1;
  }

  private boolean keyEqual(KeyClass one, KeyClass two) {
    if(one instanceof IntegerKey) {
      return ((IntegerKey) one).getKey().equals(((IntegerKey) two).getKey());
    } else if(one instanceof RealKey) {
      return ((RealKey) one).getKey().equals(((RealKey) two).getKey());
    } else {
      return ((StringKey) one).getKey().equals(((StringKey) two).getKey());
    }
  }

  private String getJoinKeyValue(KeyClass key) {
    if(key instanceof IntegerKey)
      return ((IntegerKey)key).getKey().toString();
    else if(key instanceof RealKey)
      return ((RealKey)key).getKey().toString();
    else
      return ((StringKey)key).getKey();
  }

  private KeyClass getJoinKey(Tuple t, boolean outer) throws FieldNumberOutOfBoundException, IOException {
    int joinAttr;
    if (outer) {
      joinAttr = outer_join_attr;
    } else {
      joinAttr = inner_join_attr;
    }
    switch (outer_in[outer_join_attr - 1].attrType) {
      case AttrType.attrInteger:
        return new IntegerKey(t.getIntFld(joinAttr));
      case AttrType.attrReal:
        return new RealKey(t.getFloFld(joinAttr));
      case AttrType.attrString:
        return new StringKey(t.getStrFld(joinAttr));
    }
    return null;
  }

  private KeyClass getJoinKeyFromMetaData(Tuple t) throws FieldNumberOutOfBoundException, IOException {
    switch (metaDataAttrTypes[0].attrType) {
      case AttrType.attrInteger:
        return new IntegerKey(t.getIntFld(1));
      case AttrType.attrReal:
        return new RealKey(t.getFloFld(1));
      case AttrType.attrString:
        return new StringKey(t.getStrFld(1));
    }
    return null;
  }

  private float getMergeAttr(Tuple t ,boolean outer) throws FieldNumberOutOfBoundException, IOException {
    int mergeAttrType, mergeAttrIndex;
    if (outer) {
      mergeAttrType = outer_in[outer_merge_attr-1].attrType;
      mergeAttrIndex = outer_merge_attr;
    } else {
      mergeAttrType = inner_in[inner_merge_attr-1].attrType;
      mergeAttrIndex = inner_merge_attr;
    }
    switch (mergeAttrType) {
      case AttrType.attrInteger:
        return t.getIntFld(mergeAttrIndex);
      case AttrType.attrReal:
        return t.getFloFld(mergeAttrIndex);
    }
    return -1;
  }

  @Override
  public void close() throws IOException, JoinsException, SortException, IndexException {

    nlj.close();
    //Delete temporary heap file
    try {
      joinKeyFile.deleteFile();
      outerCandidateFile.deleteFile();
      innerCandidateFile.deleteFile();
    } catch (Exception e) {
      throw new IOException("Heap file error", e);
    }

    //Free buffer pages
    try {
      free_buffer_pages(n_pages, bufferPIDs);
    } catch (Exception e) {
      throw new IOException("Buffer Manager error", e);
    }
  }
}
