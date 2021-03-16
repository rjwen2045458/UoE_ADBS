package ed.inf.adbs.lightdb;

/**
 * The class creates a data structure table+column
 * one string for table name
 * one string for column name
 */
public class TableColumn {
    String tableName;
    String columnName;

    /**
     * constructor for string in form of "tableName.columnName"
     * @param content
     */
    TableColumn(String content){
        String[] temp = content.split("\\.");
        this.tableName = temp[0];
        this.columnName = temp[1];
    }

    /**
     * constructor for two strings
     * @param tableName
     * @param columnName
     */
    TableColumn(String tableName, String columnName){
        this.tableName = tableName;
        this.columnName = columnName;
    }

    /**
     * @return table name
     */
    public String getTableName(){
        return tableName;
    }

    /**
     * @return column name getter
     */
    public String getColumnName(){
        return columnName;
    }

    /**
     * overrides equals
     * @param tableColumn another tableColumn
     * @return true if equal, false otherwise
     */
    @Override
    public boolean equals(Object tableColumn){
        if(tableName.equals(((TableColumn)tableColumn).getTableName()) && columnName.equals(((TableColumn)tableColumn).getColumnName())){
            return true;
        }
        else{
            return false;
        }
    }

    /**
     * override toString
     * @return string
     */
    @Override
    public String toString(){
        return tableName + "." + columnName;
    }
}
