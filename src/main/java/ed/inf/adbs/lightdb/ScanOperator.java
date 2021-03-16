package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * ScanOperator is the basic operator of all the operators
 * ScanOperator reads tuples from table
 */
public class ScanOperator extends Operator{
    String databaseDir;
    HashMap<String,String[]> schema;
    String fromTableName;
    String tableDir;
    FileReader fr;
    BufferedReader br;
    String curLine;

    /**
     * Constructor
     * @param databaseDir
     * @param schema
     * @param fromTableName
     */
    ScanOperator(String databaseDir, HashMap<String,String[]> schema, String fromTableName){
        this.databaseDir = databaseDir;
        this.schema = schema;
        this.fromTableName = fromTableName;
        if(SQLInterpreter.hasAliases){
            this.tableDir = databaseDir + "/data/" + SQLInterpreter.aliases2Table.get(fromTableName) + ".csv";
        }
        else{
            this.tableDir = databaseDir + "/data/" + this.fromTableName + ".csv";
        }
        reset();
    }

    /**
     * reads next tuple
     * @return
     */
    @Override
    Tuple getNextTuple() {
        if(curLine != null){
            String[] columnNames = schema.get(fromTableName);
            String[] data = curLine.split(",");
            LongValue[] values = new LongValue[columnNames.length];
            for (int i = 0; i < columnNames.length; i++){
                values[i] = new LongValue(Integer.parseInt(data[i]));
            }
            Tuple t = new Tuple(fromTableName, columnNames, values);
            curLine = nextLine();
            return t;
        }
        else{
            return null;
        }
    }

    /**
     * initialise file readers
     */
    @Override
    void reset(){
        try{
            this.fr = new FileReader(tableDir);
            this.br = new BufferedReader(fr);
            this.curLine = nextLine();
        }
        catch (FileNotFoundException fnfE){
            fnfE.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * recursively call getNextTuple()
     * @return
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
     * reads next line in file
     * @return
     */
    String nextLine() {
        String line = null;
        try{
            line = br.readLine();
        } catch (IOException ioE){
            line = null;
        }
        return line;
    }
}
