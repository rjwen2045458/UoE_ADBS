package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.LinkedList;
import java.util.Queue;

/**
 * myExpressionDeParser extends ExpressionDeParser
 * it uses queue supporting calculation
 * it overrides visit() of (and == != > < >= <= extractValue)
 * for most of them, the overrides are straightforward, so they have no explanation
 * for the most important "extractValue" visit() overriding, there will be specific explanation
 */
public class myExpressionDeParser extends ExpressionDeParser {

    final Queue<Long> queueValue = new LinkedList<Long>();
    final Queue<Boolean> queueBoolean = new LinkedList<Boolean>();
    Tuple tuple;
    Tuple rightTuple;
    String[] leftTables;
    String rightTable;
    Boolean isJoin;

    /**
     * constructor for select operator
     * @param tuple
     */
    myExpressionDeParser(Tuple tuple){
        this.tuple = tuple;
        this.isJoin = false;
    }

    /**
     * constructor for join operator
     * @param tuple
     * @param rightTuple
     * @param leftTables
     * @param rightTable
     */
    myExpressionDeParser(Tuple tuple, Tuple rightTuple, String[] leftTables, String rightTable){
        this.tuple = tuple;
        this.rightTuple = rightTuple;
        this.leftTables = leftTables;
        this.rightTable = rightTable;
        this.isJoin = true;
    }

    @Override
    public void visit(AndExpression andExpression){
        super.visit(andExpression);

        boolean bool1 = queueBoolean.poll();
        boolean bool2 = queueBoolean.poll();

        queueBoolean.offer(bool1 && bool2);
    }

    @Override
    public void visit(EqualsTo equalsTo){
        super.visit(equalsTo);

        long value1 = queueValue.poll();
        long value2 = queueValue.poll();

        if (value1 == Long.MIN_VALUE || value2 == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(value1 == value2);
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo){
        super.visit(notEqualsTo);

        long value1 = queueValue.poll();
        long value2 = queueValue.poll();

        if (value1 == Long.MIN_VALUE || value2 == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(value1 != value2);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan){
        super.visit(greaterThan);

        long value1 = queueValue.poll();
        long value2 = queueValue.poll();

        if (value1 == Long.MIN_VALUE || value2 == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(value1 > value2);
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals){
        super.visit(greaterThanEquals);

        long value1 = queueValue.poll();
        long value2 = queueValue.poll();

        if (value1 == Long.MIN_VALUE || value2 == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(value1 >= value2);
        }
    }

    @Override
    public void visit(MinorThan minorThan){
        super.visit(minorThan);

        long value1 = queueValue.poll();
        long value2 = queueValue.poll();

        if (value1 == Long.MIN_VALUE || value2 == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(value1 < value2);
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals){
        super.visit(minorThanEquals);

        long value1 = queueValue.poll();
        long value2 = queueValue.poll();

        if (value1 == Long.MIN_VALUE || value2 == Long.MIN_VALUE){
            queueBoolean.offer(true);
        }
        else {
            queueBoolean.offer(value1 <= value2);
        }
    }

    @Override
    public void visit(LongValue longValue){
        super.visit(longValue);
        queueValue.offer(longValue.getValue());
    }

    /**
     * for select operator, it is straight forward
     *
     * for join operator
     *  for a specific column
     *   it figure out whether it should be processed currently
     *    if the column belongs to "rooter" operator in query tree
     *      push a value of Long.MIN_VALUE as signal
     *      so evaluation do not process it
     *    if the column belongs to current operator or "leafer" operator in query tree
     *      push its exact value
     *      so evaluation will process it as normal
     * @param column
     */
    @Override
    public void visit(Column column){
        super.visit(column);
        String tableName = column.getTable().getName();
        String columnName = column.getColumnName();
        TableColumn tableColumn = new TableColumn(tableName,columnName);
        if(isJoin){
            boolean inTables = rightTuple.containsColumn(tableColumn) || tuple.containsColumn(tableColumn);
            if (inTables){
                if (rightTuple.containsColumn(tableColumn)){
                    queueValue.offer(rightTuple.get(tableColumn).getValue());
                }
                else{
                    queueValue.offer(tuple.get(tableColumn).getValue());
                }
            }
            else{
                queueValue.offer(Long.MIN_VALUE);
            }
        }
        else{
            if (tuple.containsColumn(tableColumn)){
                queueValue.offer(tuple.get(tableColumn).getValue());
            }
            else{
                queueValue.offer(Long.MIN_VALUE);
            }
        }
    }

    /**
     * @return evaluating result
     */
    public boolean getEvaluationResult(){
        return queueBoolean.poll();
    }
}
