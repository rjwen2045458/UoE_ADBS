package ed.inf.adbs.lightdb;

import java.io.FileNotFoundException;
import java.util.ArrayList;

public abstract class Operator{
    abstract Tuple getNextTuple();
    abstract void reset();
    abstract ArrayList<Tuple> dump();
}
