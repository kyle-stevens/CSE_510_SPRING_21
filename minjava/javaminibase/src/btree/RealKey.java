package btree;

public class RealKey  extends KeyClass {

  private Float key;

  public String toString(){
     return key.toString();
  }

  /** Class constructor
   *  @param     value   the value of the real key to be set 
   */
  public RealKey(Float value) 
  { 
    key=new Float(value.floatValue());
  }

  /** Class constructor
   *  @param     value   the value of the real key to be set 
   */
  public RealKey(float value) 
  { 
    key=new Float(value);
  }



  /** get a copy of the real key
   *  @return the reference of the copy 
   */
  public Float getKey() 
  {
    return new Float(key.intValue());
  }

  /** set the real key value
   */  
  public void setKey(Float value) 
  { 
    key=new Float(value.floatValue());
  }
}
