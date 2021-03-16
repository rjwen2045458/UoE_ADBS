package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * SelectOperator accepts tuples from scan operator
 * SelectOperator processes SQL keyword WHERE
 */
public class SelectOperator extends Operator{
    HashMap<String,String[]> schema;
    ScanOperator scanOperator;
    Expression whereExpression;

    /**
     * constructor
     * @param databaseDir
     * @param schema
     * @param fromTableName
     * @param whereExpression
     */
    SelectOperator(String databaseDir, HashMap<String,String[]> schema, String fromTableName, Expression whereExpression){
        this.schema = schema;
        this.scanOperator = new ScanOperator(databaseDir, schema, fromTableName);
        this.whereExpression = whereExpression;
    }

    /**
     * get next scanned tuple and check if it satisfies where expression
     * @return a satisfied tuple
     */
    @Override
    Tuple getNextTuple() {
        Tuple tuple = scanOperator.getNextTuple();
        while(tuple != null && !evaluate(tuple, whereExpression)){
            tuple = scanOperator.getNextTuple();
        };
        return tuple;
    }

    /**
     * reset scan operator
     */
    @Override
    void reset(){
        scanOperator.reset();
    }

    /**
     * recursively call getNextTuple()
     * @return result tuples
     */
    @Override
    ArrayList<Tuple> dump() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Tuple tuple = getNextTuple();
        while(tuple != null){
            tuples.add(tuple);
            tuple = getNextTuple();
        }
        return tuples;
    }

    /**
     * checks if tuple satisfies where expression
     * @param tuple target tuple
     * @param whereExpression
     * @return true if satisfies, false otherwise
     */
    public boolean evaluate(Tuple tuple, Expression whereExpression){
        myExpressionDeParser myDeparser = new myExpressionDeParser(tuple);
        StringBuilder b = new StringBuilder();
        myDeparser.setBuffer(b);
        whereExpression.accept(myDeparser);
        return myDeparser.getEvaluationResult();
    }
}
