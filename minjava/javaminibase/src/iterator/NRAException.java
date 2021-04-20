package iterator;

import chainexception.*;
import java.lang.*;

public class NRAException extends ChainException {
  public NRAException(String s){super(null,s);}
  public NRAException(Exception prev, String s){ super(prev,s);}
}
