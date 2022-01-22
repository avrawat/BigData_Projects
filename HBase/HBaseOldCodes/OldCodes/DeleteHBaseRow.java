import java.io.IOExpection
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.conf.Configuration;


public class HBaseAPI{


	Configuration conf = HBaseConfiguration.create();
	HBaseAdmin ad = new HBaseAdmin(conf);
	HTableDescriptor htable = new HTableDescriptor("RawatKiTable");

	htable.addFamily( new HColumnDescriptor("Personal"));
	htable.addFamily( new HColumnDescriptor("ContactDetails"));
	htable.addFamily( new HColumnDescriptor("Employement"));
	System.out.prinln("Connecting to the server");
	HBaseAdmin hbase_admin = new HBaseAdmin( conf );
	System.out.println("Creating the table");
	hbase_admin.createTable(htable);
	System.out.println("Table is created");




}



