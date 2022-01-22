package crud;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;


public class Driver {
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "54.152.184.4");
		conf.set("hbase.znode.parent", "/hbase");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		System.out.println("Connecting...");
		Connection con = ConnectionFactory.createConnection(conf);
		System.out.println("Connected \nCreating Table...");
		
		
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("TbaleViaJar"));
		Admin admin = con.getAdmin();
		admin.createTable(htable);
		System.out.println("Table Created");

		
		
/*		Table table = con.getTable(TableName.valueOf("RawatKiTable"));
	
		Scan scan1 = new Scan();
		System.out.println("Getting Scanner...");
		
		ResultScanner scanner1 = table.getScanner(scan1);
		
		System.out.println("Printing the Values...");
		
		for(Result res : scanner1 ) {
				
			System.out.println(Bytes.toString( res.getValue("Personal".getBytes(), "Name".getBytes())));
			System.out.println(Bytes.toString( res.getValue("Personal".getBytes(), "Age".getBytes())));
			
		}
*/		
		con.close();
		System.out.println("Connection Closed.\nProgram Terminated");
		
	}
		

}
