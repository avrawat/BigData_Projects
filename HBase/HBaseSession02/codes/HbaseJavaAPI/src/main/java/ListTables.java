/*
 * Lists all the HBase tables
*/

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ListTables {
	
	public static void main(String[] args) throws IOException {
		
		//Setting logging level to Warning
		Logger.getRootLogger().setLevel(Level.WARN);
		
		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		System.out.println("Connected");
		Admin admin = con.getAdmin();
		
		
		// get the list of all HBase tables 
		TableName[] tableList = admin.listTableNames();
		
		// if no HBase table is present 
		if(tableList.length==0) {
			System.out.println("No HBase table present");
			System.out.println("Exiting Program");
			System.exit(0);
		}
		
		System.out.println("List of HBase tables...");
		// Print names of all HBase tables 
		for (TableName val : tableList) {
			System.out.println(val.getNameAsString());
			
		}
		
		//Closing the Connection
		con.close();
		System.out.println("Exiting Program");

	}

}