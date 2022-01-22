/*
 * Takes Input from the user and deletes the table
*/


import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DeleteTable {

	public static String getTableName(Admin admin) throws IOException {

		// The list of all HBase tables
		TableName[] tableList = admin.listTableNames();
		System.out.println("Select a Table...");

		// If there are no HBase table present
		if (tableList.length == 0) {
			System.out.println("No HBase table present to delete");
			System.out.println("Exiting Program");
			System.exit(0);
		}

		int sel = 0;
		for (TableName val : tableList) {
			System.out.println("Enter " + sel++ + " for ----> " + val.getNameAsString());
		}

		Scanner sc = new Scanner(System.in);
		// get input from user
		int input = sc.nextInt();

		if (input < 0 || input > tableList.length - 1) {
			System.out.println("Invaild Input");
			System.out.println("Exiting Program");
			System.exit(0);
		}
		sc.close();
		return tableList[input].getNameAsString();
	}

	public static void main(String[] args) throws IOException {
		
		// setting log level to Warnning
		Logger.getRootLogger().setLevel(Level.WARN);

		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		System.out.println("Connected");

		String table = getTableName(admin);

		// Disable the table
		System.out.println("Disabling table '" + table+"'");
		admin.disableTable(TableName.valueOf(table));

		// drop/delete the table
		System.out.println("Deleting table '" + table+"'");
		admin.deleteTable(TableName.valueOf(table));

		System.out.println("'"+table + "' table is deleted");

		// closing the connection
		con.close();
		System.out.println("Exiting Program");

	}

}
