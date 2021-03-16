package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * DuplicateEliminationOperator accepts tuples from SortOperator
 * DuplicateEliminationOperator processes SQL keyword DISTINCT
 */
public class DuplicateEliminationOperator extends Operator{
    SortOperator sortOperator;
    boolean distinct;

    /**
     * Constructor
     * @param databaseDir
     * @param schema
     * @param fromTableName
     * @param joinTableNames
     * @param columnsInfo
     * @param whereExpression
     * @param orderBy
     * @param distinct
     */
    DuplicateEliminationOperator(String databaseDir, HashMap<String,String[]> schema, String fromTableName, String[] joinTableNames, String[] columnsInfo, Expression whereExpression, String[] orderBy, boolean distinct){
        this.sortOperator = new SortOperator(databaseDir, schema, fromTableName, joinTableNames, columnsInfo, whereExpression, orderBy);
        this.distinct = distinct;
    }

    /**
     * Please use dump() on DuplicateEliminationOperator to get results
     * @return null
     */
    @Override
    Tuple getNextTuple() {
        System.out.println("Please use dump() on DuplicateEliminationOperator to get results.");
        return null;
    }

    /**
     * reset
     */
    @Override
    void reset() {
        sortOperator.reset();
    }

    /**
     * eliminates duplicates on tuples from sort operator
     * @return processed tuples
     */
    @Override
    ArrayList<Tuple> dump() {
        ArrayList<Tuple> tuples = sortOperator.dump();
        if (distinct){
            return duplicateEliminate(tuples);
        }
        else{
            return tuples;
        }
    }

    /**
     * use tuple equals() method to eliminate duplicates on sorted tuples
     * @param tuples
     * @return processed tuples
     */
    public ArrayList<Tuple> duplicateEliminate(ArrayList<Tuple> tuples){
        if(tuples == null){
            return null;
        }
        else {
            Tuple tuple = null;
            ArrayList<Tuple> newTuple = new ArrayList<Tuple>();
            for(Tuple t:tuples){
                if (tuple == null){
                    tuple = t;
                    newTuple.add(t);
                }
                else if(t.equals(tuple)){

                }
                else{
                    tuple = t;
                    newTuple.add(t);
                }
            }
            return newTuple;
        }
    }
}
