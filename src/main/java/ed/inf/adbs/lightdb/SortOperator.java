package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * SortOperator accepts tuples from project operator
 * Sort operator processes SQL keywords ORDER BY
 */
public class SortOperator extends Operator{

    ProjectOperator projectOperator;
    TableColumn[] orderBy;

    /**
     * Constructor
     * According to different situations, creates different types of projection operators
     * It determines differentiation to scan/select/join operator
     *
     * @param databaseDir
     * @param schema
     * @param fromTableName
     * @param joinTableNames
     * @param columnsInfo
     * @param whereExpression
     * @param orderBy
     */
    SortOperator(String databaseDir, HashMap<String,String[]> schema, String fromTableName, String[] joinTableNames, String[] columnsInfo, Expression whereExpression, String[] orderBy){
        this.orderBy = columnsInfo2TableColumnsArray(orderBy);
        if (whereExpression == null) {
            projectOperator = new ProjectOperator(databaseDir, schema, fromTableName, columnsInfo);
        }
        else if(joinTableNames == null) {
            projectOperator = new ProjectOperator(databaseDir, schema, fromTableName, columnsInfo, whereExpression);
        }
        else {
            projectOperator = new ProjectOperator(databaseDir, schema, fromTableName, joinTableNames, columnsInfo, whereExpression);
        }
    }

    /**
     * Please use dump() on SortOperator to get results.
     * As suggested in coursework instruction, sort operator processes tuples after all the results are obtained
     * @return null
     */
    @Override
    Tuple getNextTuple() {
        System.out.println("Please use dump() on SortOperator to get results.");
        return null;
    }

    /**
     * reset
     */
    @Override
    void reset() {
        projectOperator.reset();
    }

    /**
     * Obtain all the tuples from project operator and sort them
     * @return sorted tuples according to given column order
     */
    @Override
    ArrayList<Tuple> dump() {
        ArrayList<Tuple> tuples = projectOperator.dump();
        if (orderBy == null){
            return tuples;
        }
        else{
            return sort(tuples);
        }
    }

    /**
     * Sort tuples
     * In this methods, the lambda expression and collection.sort is used for comparing
     * @param tuples
     * @return sorted tuples
     */
    public ArrayList<Tuple> sort(ArrayList<Tuple> tuples){
        if (tuples == null){
            return null;
        }
        if (orderBy == null){
            return tuples;
        }
        ArrayList<Tuple> sortedTuple = tuples;
        Collections.sort(sortedTuple, (t1, t2) -> {
            for(int i = 0; i < orderBy.length; i++) {
                TableColumn tableColumn = orderBy[i];
                if (t1.get(tableColumn).getValue() == t2.get(tableColumn).getValue()){
                    break;
                }
                return (int) (t1.get(tableColumn).getValue() - t2.get(tableColumn).getValue());
            }
            return 0;
        });
        return sortedTuple;
    }

    /**
     * Convert string array to tableColumn array
     * Support comparing
     * @param columnsInfo
     * @return tableColumns
     */
    TableColumn[] columnsInfo2TableColumnsArray(String[] columnsInfo){
        if (columnsInfo == null){
            return null;
        }
        TableColumn[] tableColumns = new TableColumn[columnsInfo.length];
        for (int i=0; i<columnsInfo.length; i++){
            tableColumns[i] = new TableColumn(columnsInfo[i]);
        }
        return tableColumns;
    }
}
