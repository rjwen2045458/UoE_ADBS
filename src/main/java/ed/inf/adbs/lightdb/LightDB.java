package ed.inf.adbs.lightdb;


/**
 * Lightweight in-memory database system
 *
 * LightDB class creates a SQLInterpreter instance to process databaseDir, inputFile and outputFile.
 */
public class LightDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		SQLInterpreter i = new SQLInterpreter(databaseDir,inputFile,outputFile);
		i.interpret();
	}

//
}
