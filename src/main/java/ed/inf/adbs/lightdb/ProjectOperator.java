package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * ProjectOperator accepts tuples from scan/select/join operator
 * ProjectOperator processes SQL keyword SELECT (the columns follows SELECT)
 *
 * ProjectOperator can be created corresponding to scan/select/join operator based on whether join item and where statement is null
 * boolean variables join and where helps to identify the current type of this project operator
 */
public class ProjectOperator extends Operator{
    SelectOperator selectOperator;
    ScanOperator scanOperator;
    JoinOperator joinOperator;
    TableColumn[] selectedColumns;
    Boolean join;
    Boolean where;

    /**
     * Constructor for scan operator
     * @param databaseDir
     * @param schema
     * @param fromTableName
     * @param columnsInfo
     */
    ProjectOperator(String databaseDir, HashMap<String,String[]> schema, String fromTableName, String[] columnsInfo){
        this.selectedColumns = columnsInfo2TableColumnsArray(columnsInfo);
        scanOperator = new ScanOperator(databaseDir,schema,fromTableName);
        this.join = false;
        this.where = false;
    }

    /**
     * Constructor for select operator
     * @param databaseDir
     * @param schema
     * @param fromTableName
     * @param columnsInfo
     * @param whereExpression
     */
    ProjectOperator(String databaseDir, HashMap<String,String[]> schema, String fromTableName, String[] columnsInfo, Expression whereExpression){
        this.selectedColumns = columnsInfo2TableColumnsArray(columnsInfo);
        selectOperator = new SelectOperator(databaseDir, schema, fromTableName, whereExpression);
        this.join = false;
        this.where = true;
    }

    /**
     * Constructor for join operator
     * @param databaseDir
     * @param schema
     * @param fromTableName
     * @param joinTableNames
     * @param columnsInfo
     * @param whereExpression
     */
    ProjectOperator(String databaseDir, HashMap<String,String[]> schema, String fromTableName, String[] joinTableNames, String[] columnsInfo, Expression whereExpression){
        this.selectedColumns = columnsInfo2TableColumnsArray(columnsInfo);
        joinOperator = new JoinOperator(databaseDir, schema, fromTableName, joinTableNames, whereExpression);
        this.join = true;
        this.where = true;
    }

    /**
     * call getNextTuple() from corresponding operator to get tuples
     * perform projection on them
     * @return one tuple
     */
    @Override
    Tuple getNextTuple() {
        Tuple tuple;
        if (join){
            tuple = joinOperator.getNextTuple();
        }
        else if (where){
            tuple = selectOperator.getNextTuple();
        }
        else{
            tuple = scanOperator.getNextTuple();
        }
        if (tuple == null){
            return null;
        }
        else{
            if (selectedColumns == null){
                return tuple;
            }
            else {
                return tuple.projection(selectedColumns);
            }
        }
    }

    /**
     * reset the corresponding operator
     */
    @Override
    void reset() {
        if (join){
            joinOperator.reset();;
        }
        if (where){
            selectOperator.reset();
        }
        else{
            scanOperator.reset();
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
     * Convert string array to tableColumn array
     * Support projection
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
