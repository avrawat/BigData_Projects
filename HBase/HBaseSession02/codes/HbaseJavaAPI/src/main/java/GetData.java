/*
 * Prints data of the table 
*/

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GetData {

	public static void main(String[] args) throws IOException {

		// Setting logging level to Warnning
		Logger.getRootLogger().setLevel(Level.WARN);
		
		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		System.out.println("Connected");
		Admin admin = con.getAdmin();

		//Name of the table
		String table = "Employee";

		// Check if table exists
		if (admin.tableExists(TableName.valueOf(table))) {

			// get a Table object for the table
			Table htable = con.getTable(TableName.valueOf(table));
			
			//Creating a Scan object to read complete table
			Scan scan = new Scan();
			
			 
/*			
 * 			//To limit number rows to be scanned
 * 			Scan scan = new Scan(byte[] startRow)
 * 			Scan scan = new Scan(byte[] startRow, byte[] stopRow)
 * 			
 * 			// in case of big Tables
 * 				// set number of rows to cache before returning the result
 * 				scan.setCaching(int caching)
 *				
 *				//To limit the number of columns(for wind tables)
 *				scan.setBatch(int batch)	
*/
				
			ResultScanner scanner = htable.getScanner(scan);
			
			System.out.println();
			
			for (Result res : scanner) {
				
				// prints data from 'Personal' column family
					System.out.println(Bytes.toString(res.getRow()) + " "
							+ Bytes.toString(res.getValue(Bytes.toBytes("Personal"), Bytes.toBytes("Name"))) + " "
						+ Bytes.toString(res.getValue(Bytes.toBytes("Personal"), Bytes.toBytes("Age"))) + " "
						+ Bytes.toString(res.getValue(Bytes.toBytes("Personal"), Bytes.toBytes("Gender"))));
				
				// prints data from 'Contact' column family
				System.out.println(Bytes.toString(res.getRow()) + " "
						+ Bytes.toString(res.getValue(Bytes.toBytes("Contact"), Bytes.toBytes("Mobile"))) + " "
						+ Bytes.toString(res.getValue(Bytes.toBytes("Contact"), Bytes.toBytes("Email"))));
				
				// prints data from 'Employment' column family
				System.out.println( Bytes.toString(res.getRow()) + " "
						+ Bytes.toString(res.getValue(Bytes.toBytes("Employment"), Bytes.toBytes("Company"))) + " "
						+ Bytes.toString(res.getValue(Bytes.toBytes("Employment"), Bytes.toBytes("DOJ"))) + " "
						+ Bytes.toString(res.getValue(Bytes.toBytes("Employment"), Bytes.toBytes("Designation"))));	
				
				System.out.println();
			}

		} else {

			System.out.println("The HBase Table named '" + table + "' doesn't exists.");
		}
		
		// Closing the connection
		con.close();
		System.out.println("Exiting Program");

	}

}
