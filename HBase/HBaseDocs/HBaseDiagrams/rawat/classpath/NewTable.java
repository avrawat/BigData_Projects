
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

public class Driver {

	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("TableViaJar"));

		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();

		htable.addFamily(new HColumnDescriptor("Id"));
		htable.addFamily(new HColumnDescriptor("Name"));
		System.out.println("Connecting...");
		System.out.println("Creating Table...");
		admin.createTable(htable);
		System.out.println("Done!");

	}
}
