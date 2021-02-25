package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import index.*;
import java.lang.*;
import java.io.*;

import static global.GlobalConst.MAX_SPACE;

/**
 *  This file contains an implementation of the nested loops skyline
 *  algorithm.
 */

public class NestedLoopsSky extends Iterator
{
    private AttrType _in1[];
    private   int        in1_len;
    private   Iterator  outer;
    private   short t1_str_sizescopy[];
    private   int        n_buf_pgs;        // # of buffer pages available.
    private   boolean        done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private Heapfile hf;
    private Scan inner;
    private int pref_list[];
    private int pref_list_length;
    private int n_pages;
    private boolean dominated;

    /**constructor
     *@param in1  Array containing field types of R.
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields.
     *@param am1  access method for input data
     *@param relationName  access hfapfile for input data
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     */
    public NestedLoopsSky( AttrType    in1[],
                           int     len_in1,
                           short   t1_str_sizes[],
                           Iterator     am1,
                           String relationName,
                           int   pref_list[],
                           int    pref_list_length,
                           int n_pages
    ) throws NestedLoopException
    {
        _in1 = new AttrType[in1.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        in1_len = len_in1;
        outer = am1;
        t1_str_sizescopy =  t1_str_sizes;
        inner_tuple = new Tuple();
        inner = null;
        done  = false;
        get_from_outer = true;
        dominated = false;
        this.pref_list = pref_list;
        this.pref_list_length = pref_list_length;
        this.n_pages = n_pages;

        try {
            hf = new Heapfile(relationName);
        }
        catch(Exception e) {
            throw new NestedLoopException(e, "Create new heapfile failed.");
        }
    }

    /**
     *@return The skyline tuple is returned
     *@exception NestedLoopException exception from this class
     *@exception Exception other exceptions
     */
    public Tuple get_next()
            throws NestedLoopException,
            Exception
    {
        if (done)
            return null;

        do
        {
            if (get_from_outer == true)
            {
                get_from_outer = false;
                if (inner != null)
                {
                    inner = null;
                }

                try {
                    inner = hf.openScan();
                }
                catch(Exception e){
                    throw new NestedLoopException(e, "openScan failed");
                }

                if ((outer_tuple=outer.get_next()) == null)
                {
                    done = true;
                    if (inner != null)
                    {
                        inner = null;
                    }
                    return null;
                }
            }

            dominated = false;

            RID rid = new RID();
            while ((inner_tuple = inner.getNext(rid)) != null)
            {
                inner_tuple.setHdr((short)in1_len, _in1,t1_str_sizescopy);
                if(TupleUtils.Equal(outer_tuple, inner_tuple, _in1, in1_len )){
                    continue;
                }
                if(TupleUtils.Dominates(inner_tuple, _in1, outer_tuple, _in1,
                        Short.parseShort(in1_len+""), t1_str_sizescopy,
                        pref_list, pref_list_length)) {
                    dominated = true;
                    break;
                }
            }

            get_from_outer = true;

            if(!dominated){
                return outer_tuple;
            }

        } while (true);
    }

    /**
     * implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error
     */
    public void close() throws IOException, IndexException {
        if (!closeFlag) {

            try {
                outer.close();
            }catch (Exception e) {
                throw new IOException("NestLoopSky.java: error in closing iterator.", e);
            }
            closeFlag = true;
        }
    }
}






