package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * JoinOperator accepts tuples from select operator or its child join operator
 * joinOperator processes SQL function join
 * JoinOperator uses left-deep join tree method
 * JoinOperator has two children, left one is a select or join operator, right one is a select operator
 * each right operator will correspond to one table
 * for left child join operator, there will create a right select operator until there are only two table left
 * then left operator will be a select operator, which reaches tree bottom
 */
public class JoinOperator extends Operator{
    String[] leftTableNames;
    String rightTableName;
    Expression whereExpression;
    JoinOperator leftJoinOperator;
    SelectOperator leftSelectOperator;
    SelectOperator rightSelectOperator;
    Boolean joinTreeBottom;
    Tuple leftTuple = null; // support getNextTuple() in join tree bottom
    Tuple rightTuple = null; // support getNextTuple() in join tree bottom

    /**
     * Constructor
     * if current tables more than 2
     *      create left child operator as join operator, right child operator as select operator
     *      total table number to left join operator minors 1
     * else
     *      both left and right operator are select operators
     *      reaches tree bottom
     * @param databaseDir
     * @param schema
     * @param rightTableName
     * @param joinTableNames
     * @param whereExpression
     */
    JoinOperator(String databaseDir, HashMap<String,String[]> schema, String rightTableName, String[] joinTableNames, Expression whereExpression){
        this.leftTableNames = joinTableNames;
        this.rightTableName = rightTableName;
        this.whereExpression = whereExpression;
        if (joinTableNames.length > 1){
            String[] newJoinTableNames = Arrays.copyOfRange(joinTableNames, 1, joinTableNames.length);
            leftJoinOperator = new JoinOperator(databaseDir, schema, joinTableNames[0], newJoinTableNames, whereExpression);
            joinTreeBottom = false;
        }
        else{
            leftSelectOperator = new SelectOperator(databaseDir, schema, joinTableNames[0], whereExpression);
            joinTreeBottom = true;
        }
        rightSelectOperator = new SelectOperator(databaseDir, schema, rightTableName, whereExpression);
    }

    /**
     * get a tuple from left select/join operator
     * recursively get a tuple from right select operator satisfies conditions
     * after one loop from right operator, get next tuple from left operator
     * if left operator is join operator
     *      it will iteratively call next left operator`s getNextTuple() to obtain a tuple
     *      until left operator is a select operator
     * perform join on two obtained tuples
     * @return result tuple
     */
    @Override
    Tuple getNextTuple() {
        Operator leftOperator = null;
        if (joinTreeBottom){
            leftOperator = leftSelectOperator;
        }
        else{
            leftOperator = leftJoinOperator;
        }
        if (leftTuple == null) {
            leftTuple = leftOperator.getNextTuple();
        }
        rightTuple = rightSelectOperator.getNextTuple();
        if (rightTuple == null) {
            rightSelectOperator.reset();
            rightTuple = rightSelectOperator.getNextTuple();
            leftTuple = leftOperator.getNextTuple();
        }
        while(leftTuple != null && !evaluate(leftTuple,rightTuple,leftTableNames,rightTableName,whereExpression)){
            rightTuple = rightSelectOperator.getNextTuple();
            while(rightTuple != null && !evaluate(leftTuple,rightTuple,leftTableNames,rightTableName,whereExpression)){
                rightTuple = rightSelectOperator.getNextTuple();
            }
            if (rightTuple == null){
                rightSelectOperator.reset();
                rightTuple = rightSelectOperator.getNextTuple();
                leftTuple = leftOperator.getNextTuple();
            }
        }
        if (leftTuple == null){
            return null;
        }
        else {
            return leftTuple.join(rightTuple);
        }
    }

    /**
     * reset child operator and class global variables
     */
    @Override
    void reset() {
        if(joinTreeBottom) {
            leftTuple = null;
            rightTuple = null;
            leftSelectOperator.reset();
            rightSelectOperator.reset();
        }
        else{
            leftTuple = null;
            rightTuple = null;
            leftJoinOperator.reset();
            rightSelectOperator.reset();
        }
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
     * evaluate if left tuple and right tuple satisfy the where expression
     * @param leftTuple
     * @param rightTuple
     * @param leftTables all names of tables processed in left branch
     * @param rightTable name of table processed in right branch
     * @param whereExpression
     * @return true if satisfy, false otherwise
     */
    public boolean evaluate(Tuple leftTuple, Tuple rightTuple, String[] leftTables, String rightTable, Expression whereExpression){
        myExpressionDeParser myDeparser = new myExpressionDeParser(leftTuple, rightTuple, leftTables, rightTable);
        StringBuilder b = new StringBuilder();
        myDeparser.setBuffer(b);
        whereExpression.accept(myDeparser);
        return myDeparser.getEvaluationResult();
    }
}
