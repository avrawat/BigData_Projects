
/*
 * Creates a new HBase Table
*/


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CreateTable {

	public static void main(String[] args) throws IOException {

		// Setting logging level to Warnnings.
		Logger.getRootLogger().setLevel(Level.WARN);

		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		System.out.println("Connected");
		Admin admin = con.getAdmin();
		// Name of the HBase Table
		String table = "Employee";
		//get a table descriptor object
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(table));
	    // add columns to the table
		htable.addFamily(new HColumnDescriptor("Personal"));
		htable.addFamily(new HColumnDescriptor("Contact"));
		htable.addFamily(new HColumnDescriptor("Employment"));

		System.out.println("Creating table named '" + table+"'");
		admin.createTable(htable);
		System.out.println("'"+table + "' table is created");

		// Closing the connection
		con.close();
		System.out.println("Exiting Program");

	}
}
