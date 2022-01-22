import java.io.IOException;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.conf.Configuration;


public class creatingTable{

    public static void main(String args[])throws IOException{
	HBaseConfiguration conf = new HBaseConfiguration(new Configuration());

	HTableDescriptor htable = new HTableDescriptor("RawatKiTable");

	htable.addFamily( new HColumnDescriptor ( "Personal") );
	htable.addFamily( new HColumnDescriptor("ContactDetails"));
	htable.addFamily( new HColumnDescriptor("Employement"));



	System.out.println("Connecting to the server...");
	HBaseAdmin hbase_admin = new HBaseAdmin( conf );
	System.out.println("Creating the table...");
	hbase_admin.createTable(htable);

	System.out.println("Table is created");


	

     }


}



