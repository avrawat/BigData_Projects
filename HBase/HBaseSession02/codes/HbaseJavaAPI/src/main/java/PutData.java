
/*
 * Populates Data into a existing HBase table 
*/
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PutData {

	public static void main(String[] args) throws IOException {

		//Setting logging level to Warnning
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

			//Get a Table object
			Table htable = con.getTable(TableName.valueOf(table));
			

			/*********** adding a new row ***********/

			// Define row key
			Put p = new Put(Bytes.toBytes("row1"));

			// p.addColumn(byte[] ColumnFamily, byte[] CoulmnQualifier, long TimeStamp, byte[] value);

			p.addColumn(Bytes.toBytes("Contact"), Bytes.toBytes("Mobile"), Bytes.toBytes("9876543210"));
			p.addColumn(Bytes.toBytes("Contact"), Bytes.toBytes("Email"), Bytes.toBytes("abhc@gmail.com"));

			p.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Name"), Bytes.toBytes("Abhinav Rawat"));
			p.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Age"), Bytes.toBytes("21"));
			p.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Gender"), Bytes.toBytes("M"));

			p.addColumn(Bytes.toBytes("Employment"), Bytes.toBytes("Company"), Bytes.toBytes("Google"));
			p.addColumn(Bytes.toBytes("Employment"), Bytes.toBytes("DOJ"), Bytes.toBytes("11:06:2018"));
			p.addColumn(Bytes.toBytes("Employment"), Bytes.toBytes("Designation"), Bytes.toBytes("Senior VP"));
			
			// add the row to the table
			htable.put(p);

			/**********************/

			p = new Put(Bytes.toBytes("row2"));

			p.addColumn(Bytes.toBytes("Contact"), Bytes.toBytes("Mobile"), Bytes.toBytes("1234567890"));
			p.addColumn(Bytes.toBytes("Contact"), Bytes.toBytes("Email"), Bytes.toBytes("abc@gmail.com"));

			p.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Name"), Bytes.toBytes("Utkarsh Singh"));
			p.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Age"), Bytes.toBytes("45"));
			p.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Gender"), Bytes.toBytes("M"));

			p.addColumn(Bytes.toBytes("Employment"), Bytes.toBytes("Company"), Bytes.toBytes("Singh Computers Inc."));
			p.addColumn(Bytes.toBytes("Employment"), Bytes.toBytes("DOJ"), Bytes.toBytes("05:05:2009"));
			p.addColumn(Bytes.toBytes("Employment"), Bytes.toBytes("Designation"), Bytes.toBytes("Founder"));

			htable.put(p);

			/**********************/

			p = new Put(Bytes.toBytes("row3"));

			p.addColumn(Bytes.toBytes("Contact"), Bytes.toBytes("Mobile"), Bytes.toBytes("9988776600"));
			p.addColumn(Bytes.toBytes("Contact"), Bytes.toBytes("Email"), Bytes.toBytes("xyz@gmail.com"));

			p.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Name"), Bytes.toBytes("Steve Rogers"));
			p.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Age"), Bytes.toBytes("90"));
			p.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Gender"), Bytes.toBytes('M'));

			p.addColumn(Bytes.toBytes("Employment"), Bytes.toBytes("Company"), Bytes.toBytes("Avengers"));
			p.addColumn(Bytes.toBytes("Employment"), Bytes.toBytes("DOJ"), Bytes.toBytes("05:05:2011"));
			p.addColumn(Bytes.toBytes("Employment"), Bytes.toBytes("Designation"), Bytes.toBytes("Captain"));

			htable.put(p);

			/**********************/

			System.out.println("Table '" + table + "' is Populated");

		} else {

			System.out.println("The HBase Table named '" + table + "' doesn't exists.");
		}
		
		//Closing the connection
		con.close();
		System.out.println("Exiting Program");

	}

}
