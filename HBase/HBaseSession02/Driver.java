
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.*;

public class Driver {

	private static Configuration conf;
	private static Connection conn;
	private static Admin admin;
	private static Scanner sc;
	private static TableName tableName;

	// Creates a table
	public static void createTable() throws IOException {

		String tableName; // name of the table
		String[] cfs; // list of column family names
		String temp; // dummy variable

		System.out.println("Enter the table name...");
		tableName = sc.next();

		if (admin.tableExists(TableName.valueOf(tableName))) {
			System.out.println("The named " + tableName + " already Exists!");
			return;
		}

		System.out.println("Creating '" + tableName + "' table...");
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(tableName));

		System.out.println("Enter the number of column families");
		int n = sc.nextInt();

		cfs = new String[n];

		System.out.println("Enter the name of " + n + " column familes...");
		for (int i = 0; i < n; i++) {
			temp = sc.next();
			cfs[i] = temp;
		}

		for (String cf : cfs) {
			System.out.println("Adding column family named '" + cf + "' to the '" + tableName + "' table");
			htable.addFamily(new HColumnDescriptor(cf));
		}

		// create the table
		admin.createTable(htable);
		System.out.println("'" + tableName + "' table is created");

	}// CreateTable
	
	// puts data into a table
	public static void putData() throws IOException {

		// Get a Table object
		Table table = conn.getTable(tableName);

		// Print the column familes
		printCFs();

		System.out.println("Enter 'exit' to exit");
		System.out.println("Input comma seprated value: RowKey,ColumnFamily,ColumnQualifier,Value");

		String[] inputString = null;

		sc.nextLine();
		String inputLine = sc.nextLine();
		inputString = inputLine.split(",");

		while (!inputString[0].equals("exit")) {

			if (inputString.length != 4) {
				System.out.println("Invalid input format!");
				break;
			}

			Put put = new Put(Bytes.toBytes(inputString[0]));
			put.addColumn(Bytes.toBytes(inputString[1]), Bytes.toBytes(inputString[2]), Bytes.toBytes(inputString[3]));
			table.put(put);
			inputLine = sc.nextLine();
			inputString = inputLine.split(",");
		}

		System.out.println("---------------------------");
	}
	
	// prints the complete table
	public static void printData() throws IOException {

		// Get a Table object
		Table table = conn.getTable(tableName);

		System.out.println("Enter number of row caching...");
		int caching = sc.nextInt();

		Scan scan = new Scan();
		scan.setCaching(caching);
		ResultScanner scanner = table.getScanner(scan);

		System.out.println("---------------------------");

		for (Result res : scanner) {

			for (Cell cell : res.listCells()) {

				byte[] arr = cell.getRowArray();

				System.out.print(
						Bytes.toString(Arrays.copyOfRange(arr, cell.getRowOffset(), cell.getFamilyOffset() - 1)));

				System.out.print(" "
						+ Bytes.toString(Arrays.copyOfRange(arr, cell.getFamilyOffset(), cell.getQualifierOffset())));

			
				System.out.print(" " + Bytes.toString(Arrays.copyOfRange(arr, cell.getQualifierOffset(),
						cell.getQualifierLength() + cell.getQualifierOffset())));
				
				Timestamp ts = new Timestamp(cell.getTimestamp());
				Date date = new Date(ts.getTime());
				System.out.print(" " + date);

				System.out.print(
						" " + Bytes.toString(Arrays.copyOfRange(arr, arr.length - cell.getValueLength(), arr.length)));

				System.out.println();

			}
		}

		System.out.println("---------------------------");
	}

	// deletes a table
	public static void deleteTable() throws IOException {

		// Disable the table
		System.out.println("Disabling table '" + tableName.getNameAsString() + "'");
		admin.disableTable(tableName);

		// drop/delete the table
		System.out.println("Deleting table '" + tableName.getNameAsString() + "'");
		admin.deleteTable(tableName);
		System.out.println("'" + tableName.getNameAsString() + "' table is deleted");

	}// DeleteTable

	// deletes a value
	public static void deleteValue() throws IOException {

		Table table = conn.getTable(tableName);

		System.out.println("---------------------------");
		System.out.println("Enter 'exit' to exit");
		System.out.println("To delete a value: RowKey,ColumnFamily,ColumnQualifier");

		String[] inputString = null;

		sc.nextLine();

		String inputLine = sc.nextLine();
		inputString = inputLine.split(",");

		while (!inputString[0].equals("exit")) {

			if (inputString.length != 3) {
				System.out.println("Invalid input format!");
				break;
			}

			Delete delete = new Delete(Bytes.toBytes(inputString[0]));
			delete.addColumn(Bytes.toBytes(inputString[1]), Bytes.toBytes(inputString[2]));
			table.delete(delete);
			inputLine = sc.nextLine();
			inputString = inputLine.split(",");
		}

		System.out.println("---------------------------");

	}

	// deletes a coloumn family
	public static void deleteCF() throws IOException {

		// print column familes
		printCFs();

		System.out.println("Enter a columnFamily name to delete...");
		String cf = sc.next();

		Table table = conn.getTable(tableName);

		if (!table.getTableDescriptor().hasFamily(Bytes.toBytes(cf))) {
			System.out.println("'" + cf + "' doesn't exists in the '" + table.getName().getNameAsString() + "'");
			return;
		}

		System.out.println("Deleting '" + cf + " from '" + tableName.getNameAsString() + "'...");
		admin.deleteColumn(tableName, Bytes.toBytes(cf));
		System.out.println("'" + cf + "' is deleted from '" + tableName.getNameAsString() + "'");

	}// DeleteCF

	// delete client
	public static void deleteData() throws IOException {

		System.out.println("---------------------------");
		System.out.println("Enter 1 ---> To delete '" + tableName.getNameAsString() + "' table");
		System.out.println("Enter 2 ---> To delete a columnFamily from '" + tableName.getNameAsString() + "' table");
		System.out.println("Enter 3 ---> To delete a value from '" + tableName.getNameAsString() + "' table");

		int choice = sc.nextInt();

		switch (choice) {

		case 1:
			deleteTable();
			break;

		case 2:
			deleteCF();
			break;
		case 3:
			deleteValue();
			break;

		default:
			System.out.println("Invalid Input!");
			break;

		}

	}

	// lists all the HBase tables present
	public static void listTables() throws IOException {

		// get the list of all HBase tables
		TableName[] tableList = admin.listTableNames();
		// if no HBase table is present
		if (tableList.length == 0) {
			System.out.println("No HBase table present");
			return;
		}

		System.out.println("List of HBase tables...");
		// Print names of all HBase tables
		System.out.println("---------------------------");
		for (TableName val : tableList) {
			System.out.println(val.getNameAsString());
		}
		System.out.println("---------------------------");
	}

	// prints all the column familes of a table
	public static void printCFs() throws IOException {

		HTableDescriptor tableDptr = admin.getTableDescriptor(tableName);
		System.out.println("The ColumnFamilies of table '" + tableName.getNameAsString() + "' are...");
		HColumnDescriptor[] colDes = tableDptr.getColumnFamilies();
		System.out.println("---------------------------");
		for (HColumnDescriptor cf : colDes) {
			System.out.println(cf.getNameAsString());
		}
		System.out.println("---------------------------");
	}

	// tabkes table name as input from user
	public static TableName getTableName() throws IOException {

		// The list of all HBase tables
		TableName[] tableList = admin.listTableNames();

		// If there are no HBase table present
		if (tableList.length == 0) {
			System.out.println("No HBase table is present");
			return null;
		}

		System.out.println("Select a table...");
		int sel = 0;
		for (TableName val : tableList) {
			System.out.println("Enter " + sel++ + " for ----> " + val.getNameAsString());
		}

		// get input from user
		int input = sc.nextInt();

		if (input < 0 || input > tableList.length - 1) {
			System.out.println("Invaild Input");
			return null;
		}

		return tableList[input];

	}

	// main method
	public static void main(String[] args) throws IOException {

		Logger.getRootLogger().setLevel(Level.WARN);

		// Create HBase configuration
		conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		conn = ConnectionFactory.createConnection(conf);
		System.out.println("Connected");
		admin = conn.getAdmin();

		sc = new Scanner(System.in);
		char ch = 'Y';

		while (ch == 'Y' || ch == 'y') {
			System.out.println("---------------------------");
			System.out.println("Enter 1 ---> To create a table");
			System.out.println("Enter 2 ---> To put Data into table");
			System.out.println("Enter 3 ---> To print Data of a table");
			System.out.println("Enter 4 ---> To delete data");
			System.out.println("Enter 5 ---> To list all tables");

			int choise = sc.nextInt();
			try {
				switch (choise) {

				case 1:
					createTable();
					break;

				case 2:
					tableName = getTableName();
					if (tableName != null) {
						putData();
					}
					break;

				case 3:
					tableName = getTableName();
					if (tableName != null) {
						printData();
					}
					break;

				case 4:
					tableName = getTableName();
					if (tableName != null) {
						deleteData();
					}
					break;

				case 5:
					listTables();
					break;

				default:
					System.out.println("Invalid Input!");
					break;

				}// switch case
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("Do you want to continue? : y/n");
			ch = sc.next().charAt(0);
		} // while loop

		sc.close();
		// close the connection
		conn.close();
		System.out.println("Connection Closed");
		System.out.println("Exiting program");

	}

}
