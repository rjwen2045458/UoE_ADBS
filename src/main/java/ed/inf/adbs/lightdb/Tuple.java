package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;

import java.util.LinkedHashMap;

/**
 * Data structure tuple
 * stored as linked hashmap
 * key is table and column combination
 * value is LongValue
 */
public class Tuple {
    LinkedHashMap<TableColumn,LongValue> tuple;

    /**
     * constructor for tableName, columns and values input
     * @param tableName
     * @param columnsNames
     * @param values
     */
    Tuple(String tableName, String[] columnsNames, LongValue[] values){
        LinkedHashMap<TableColumn,LongValue> tuple = new LinkedHashMap<TableColumn,LongValue>();
        for (int i = 0; i < columnsNames.length; i++){
            tuple.put(new TableColumn(tableName,columnsNames[i]), values[i]);
        }
        this.tuple = tuple;
    }

    /**
     * constructor for tableColumns and values input
     * @param tableColumns
     * @param values
     */
    Tuple(TableColumn[] tableColumns, LongValue[] values){
        LinkedHashMap<TableColumn,LongValue> tuple = new LinkedHashMap<TableColumn,LongValue>();
        for (int i = 0; i < tableColumns.length; i++){
            tuple.put(tableColumns[i],values[i]);
        }
        this.tuple = tuple;
    }

    /**
     * constructor for linked hashmap input
     * @param tuple
     */
    Tuple(LinkedHashMap<TableColumn,LongValue> tuple){
        this.tuple = tuple;
    }

    /**
     * perform tuple projection
     * @param tableColumns target tableColumn
     * @return result tuple
     */
    public Tuple projection(TableColumn[] tableColumns){
        LongValue[] values = new LongValue[tableColumns.length];
        for (int i = 0; i < tableColumns.length; i++){
            values[i] = this.get(tableColumns[i]);
        }
        return new Tuple(tableColumns, values);
    }

    /**
     * perform join
     * @param t another tuple
     * @return joined tuple
     */
    public Tuple join(Tuple t){
        LinkedHashMap<TableColumn,LongValue> newTuple = t.getTuple();
        for(TableColumn tableColumn:tuple.keySet()){
            newTuple.put(tableColumn, tuple.get(tableColumn));
        }
        return new Tuple(newTuple);
    }

    /**
     * @return linked hashmap of tuple
     */
    public LinkedHashMap<TableColumn, LongValue> getTuple(){
        return tuple;
    }

    /**
     * get value
     * @param tableColumn
     * @return value
     */
    public LongValue get(TableColumn tableColumn){
        for(TableColumn tc: tuple.keySet()){
            if (tc.equals(tableColumn)){
                return tuple.get(tc);
            }
        }
        return null;
    }

    /**
     * check if two tuples are equal
     * @param t another tuple
     * @return true if equal, false otherwise
     */
    public boolean equals(Tuple t){
        if(tuple.keySet().equals(t.getTuple().keySet())){
            for(TableColumn tc:tuple.keySet()){
                if (tuple.get(tc).getValue() != t.getTuple().get(tc).getValue()){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * @param tableColumn
     * @return if tuple contains given column
     */
    public Boolean containsColumn(TableColumn tableColumn){
        for(TableColumn tc: tuple.keySet()){
            if (tc.equals(tableColumn)){
                return true;
            }
        }
        return false;
    }

    /**
     * used for writing to files
     * @return tuple values in string
     */
    public String outputTuple(){
        String outputString = "";
        for (TableColumn tableColumn : tuple.keySet()) {
            outputString += ((int)tuple.get(tableColumn).getValue() + ",");
        }
        return outputString.substring(0,outputString.length()-1);
    }

    /**
     * override toString()
     * mainly for testing during development
     * @return tuple in string
     */
    @Override
    public String toString(){
        String tuple2String = "";
        for (TableColumn tableColumn : tuple.keySet()) {
            tuple2String += (tableColumn + " ");
        }
        tuple2String += "\n";
        for (TableColumn tableColumn : tuple.keySet()) {
            tuple2String += ((int)tuple.get(tableColumn).getValue() + " ");
        }
        return tuple2String;
    }
}
