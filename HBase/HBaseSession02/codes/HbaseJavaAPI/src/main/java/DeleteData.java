/*
 * Deletes data from a table
*/



import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DeleteData {

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
			
			// Get the Table object 
			Table htable = con.getTable(TableName.valueOf(table));
			
			// Deleting data related of a perticular row
			Delete deleteData = new Delete(Bytes.toBytes("row1"));    
				 	
			// to delete the latest version of the value < row1, ContactDetils:Mobile >
			deleteData.addColumn(Bytes.toBytes("Contact"), Bytes.toBytes("Mobile"));
			
/*		
 * 			// To delete a specific version of a value, we can specify the timestamp also
 *			deleteData.addColumn(byte[] columnFamily, byte[] columnQualifier, long timeStamp);
 * 			
 *			// To delete the entire column family of a perticular rowkey 
 *			deleteData.deleteFamily(byte[] columnFamily);
 *
 *			
*/		
			// Delete the data from the table
			htable.delete(deleteData);
			
/*			
 * 			// To delete the complete column family ( for all rows)
 * 			admin.deleteColumn(TableName, byte[] columnFamily);
*/
			System.out.println("Deleted the values");
			
		} else {
			System.out.println("The HBase Table named '" + table + "' doesn't exists.");
		}
		
		//Closing the connections
		con.close();
		System.out.println("Exiting Program");
	}

}
