



// creating a table

public class MyFirstHBaseTable{

	public static void main(String[] args) throws IOException{

		HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
		HTableDescriptor htable = new HTableDescriptor("TableDem04");

		htable.addFamily( new HColumnDescriptor("ID"));
		htable.addFamily( new HColumnDescriptor("Name"));

		System.out.println("Connecting...");

		HBaseAdmin hbase_admin = new HBaseAdmin(hconfig);

		System.out.println("Creating Table... ");

		hbase_admin.createTable(htable);

		System.out.println("Done!");



	}



}


// putting value in the Hbase table


import java.io.IOException;							
import org.apache.hadoop.hbase.HBaseConfiguration;			
import org.apache.hadoop.hbase.client.Get;							
import org.apache.hadoop.hbase.client.HTable;							
import org.apache.hadoop.hbase.client.Put;							
import org.apache.hadoop.hbase.client.Result;						
import org.apache.hadoop.hbase.client.ResultScanner;						
import org.apache.hadoop.hbase.client.Scan;						
import org.apache.hadoop.hbase.util.Bytes;	



						
public class HBaseLoading							
{							
public static void main(String[] args) throws IOException						
{							
// When you create a HBaseConfiguration, it reads in whatever you've set into your hbase-site.xml and in hbase-default.xml, as long as these can be found on the CLASSPATH 							

org.apache.hadoop.conf.Configurationconfig = HBaseConfiguration.create();							

//This instantiates an HTable object that connects you to the "test" table			

HTable table = newHTable(config, "guru99");										

// To add to a row, use Put. A Put constructor takes the name of the row you want to insert into as a byte array. 							

  Put p = new Put(Bytes.toBytes("row1"));										

//To set the value you'd like to update in the row 'row1', specify  the column family, column qualifier, and value of the table cell you'd like to update.  The column family must already exist in your table schema.  The qualifier can be anything. 							

p.add(Bytes.toBytes("education"), 

Bytes.toBytes("col1"),Bytes.toBytes("BigData"));								

p.add(Bytes.toBytes("projects"),Bytes.toBytes("col2"),Bytes.toBytes("HBaseTutorials"));

// Once you've adorned your Put instance with all the updates you want to  make, to commit it do the following 							

table.put(p);	

// Now, to retrieve the data we just wrote.	

  Get g = new Get(Bytes.toBytes("row1"));										
  Result r = table.get(g);					

byte [] value = r.getValue(Bytes.toBytes("education"),Bytes.toBytes("col1"));											
byte [] value1 = r.getValue(Bytes.toBytes("projects"),Bytes.toBytes("col2"));											
  String valueStr = Bytes.toString(value);							

String valueStr1 = Bytes.toString(value1);							
System.out.println("GET: " +"education: "+ valueStr+"projects: "+valueStr1);														

  Scan s = new Scan();								

s.addColumn(Bytes.toBytes("education"), Bytes.toBytes("col1"));										
s.addColumn(Bytes.toBytes("projects"), Bytes.toBytes("col2"));										
ResultScanner scanner = table.getScanner(s);							
try							
{							
for (Result rr = scanner.next(); rr != null; rr = scanner.next())									
   {							
System.out.println("Found row : " + rr);										
       }							
} finally							
{							
// Make sure you close your scanners when you are done!						

scanner.close();							
       }							
   }							
}


































