package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * SQLInterpreter is the core processor of LightDB system.
 * The given input sql file is interpreted.
 * The specific processing operator is created.
 * The result tuples is returned and stored to given output file.
 */
public class SQLInterpreter {
    String databaseDir;
    String inputFile;
    String outputFile;
    /**
     * hasAliases and aliases2Table supports aliases tracing
     * aliases2Table is a hashmap in which key is aliases and values is tables` original names, which transfer aliases to table names.
     */
    public static boolean hasAliases = false;
    public static HashMap<String,String> aliases2Table = new HashMap<String,String>();

    /**
     * SQLInterpreter constructor.
     * @param databaseDir args[0]
     * @param inputFile args[1]
     * @param outputFile args[2]
     */
    SQLInterpreter(String databaseDir, String inputFile, String outputFile){
        this.databaseDir = databaseDir;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    /**
     * The core function of SQLInterpreter.
     *
     * fromTableName: the from item extracted from the SQL statement
     * joinTableNames: the join tables, can be null
     * selectedColumns: the selected columns, can be null
     * whereExpression: the where statement, can be null
     * orderBy: the order list of columns, can be null
     * distinct: whether to preform duplicate elimination
     * schema: the table name and their corresponding columns
     */
    public void interpret(){
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
            if (statement != null) {
                Select select = (Select) statement;
                PlainSelect plain = (PlainSelect) select.getSelectBody();

                String fromTableName = plain.getFromItem().toString();

                String[] joinTableNames = arrayList2StringArray((ArrayList) plain.getJoins());

                String[] selectedColumns = arrayList2StringArray((ArrayList) plain.getSelectItems());
                if(selectedColumns[0] == "*"){ selectedColumns = null;}

                Expression whereExpression = plain.getWhere();

                String[] orderBy = arrayList2StringArray((ArrayList) plain.getOrderByElements());

                boolean distinct = distinctConvert(plain.getDistinct());

                HashMap<String,String[]> schema = readSchema(databaseDir);

                if(AliasCheck(fromTableName, joinTableNames)){
                    hasAliases = true;
                    schema = updateSchemaAliases2Table(fromTableName, joinTableNames, schema);
                    fromTableName = FromTableNameAlias(fromTableName);
                    joinTableNames = JoinTableNamesAliases(joinTableNames);
                }

                Operator operator = new DuplicateEliminationOperator(databaseDir, schema, fromTableName, joinTableNames, selectedColumns, whereExpression, orderBy,distinct);
                ArrayList<Tuple> result = operator.dump();
                writeOutput(outputFile,result);

            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
    }

    /**
     * Check if aliases is used in sql statements
     * @param fromTableName from table
     * @param joinTables joins tables
     * @return true if used aliases, false otherwise
     */
    public boolean AliasCheck(String fromTableName, String[] joinTables){
        if (fromTableName.contains(" ")){
            return true;
        }
        if (joinTables == null){
            return false;
        }
        for (int i = 0; i< joinTables.length; i++){
            if (joinTables[i].contains(" ")){
                return true;
            }
        }
        return false;
    }

    /**
     * Schema is updated and aliases2Table is created if used alises
     * @param fromTableName from table
     * @param joinTables join table
     * @param schema schema info
     * @return updated schema
     */
    public HashMap<String,String[]> updateSchemaAliases2Table(String fromTableName, String[] joinTables, HashMap<String,String[]> schema){
        HashMap<String,String[]> newSchema = schema;
        if (fromTableName.contains(" ")){
            String[] tableAlia = fromTableName.split(" ");
            newSchema.put(tableAlia[1], schema.get(tableAlia[0]));
            aliases2Table.put(tableAlia[1], tableAlia[0]);
        }
        if (joinTables == null){
            return newSchema;
        }
        for (int i = 0; i< joinTables.length; i++){
            String joinTableName = joinTables[i];
            if (joinTableName.contains(" ")){
                String[] tableAlia = joinTableName.split(" ");
                newSchema.put(tableAlia[1], schema.get(tableAlia[0]));
                aliases2Table.put(tableAlia[1 ], tableAlia[0]);
            }
        }
        return newSchema;
    }

    /**
     * Extract aliases
     * @param fromTableName from table
     * @return alias
     */
    public String FromTableNameAlias(String fromTableName){
        return fromTableName.split(" ")[1];
    }

    /**
     * Extract aliases
     * @param joinTableNames join tables
     * @return aliases
     */
    public String[] JoinTableNamesAliases(String[] joinTableNames){
        if (joinTableNames == null){
            return null;
        }
        else {
            String[] aliases = new String[joinTableNames.length];
            for (int i = 0; i<joinTableNames.length; i++){
                aliases[i] = joinTableNames[i].split(" ")[1];
            }
            return aliases;
        }
    }

    /**
     * Read schema from database directory and create a hashmap
     * @param databaseDir  database directory
     * @return schema as hashmap
     */
    public HashMap<String,String[]> readSchema(String databaseDir){
        HashMap<String,String[]> schema = new HashMap<String,String[]>();
        try{
            FileReader fr = new FileReader(databaseDir + "/schema.txt");
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while((line = br.readLine()) != null) {
                String[] names = line.split(" ");
                String tableName = names[0];
                String[] columns = Arrays.copyOfRange(names, 1, names.length);
                schema.put(tableName, columns);
            }
        }
        catch (FileNotFoundException fnfE){
            fnfE.printStackTrace();
            System.exit(1);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }

    /**
     * Convert arraylist to string array
     * @param list arraylist
     * @return string array
     */
    String[] arrayList2StringArray(ArrayList list){
        if (list == null){
            return null;
        }
        String[] array = new String[list.size()];
        for (int i = 0; i < list.size(); i++){
            array[i] = list.get(i).toString();
        }
        return array;
    }

    /**
     * if sql uses distinct
     * @param distinct distinct
     * @return yes if used, no otherwise
     */
    boolean distinctConvert(Distinct distinct){
        if (distinct == null){
            return false;
        }
        else{
            return true;
        }
    }

    /**
     * Write result tuples to output file location
     * @param outputFile output directory
     * @param tuples result tuples
     */
    void writeOutput(String outputFile, ArrayList<Tuple> tuples){
        File outFile = new File(outputFile);
        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
            for(Tuple tuple:tuples){
                writer.write(tuple.outputTuple());
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
