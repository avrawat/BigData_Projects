import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;


import org.apache.hadoop.conf.Configuration



import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class PopulateHBaseTable
{
  public static void main(String[] args) throws IOException
  {

  Configuration config = HBaseConfiguration.create();
  HTable table = new HTable(config, "User");
  
  Put p = new Put(Bytes.toBytes("row1"));

  p.add(Bytes.toBytes("Id"), Bytes.toBytes("col1"),Bytes.toBytes("AAA"));
  p.add(Bytes.toBytes("Name"),Bytes.toBytes("col2"),Bytes.toBytes("BBB"));

  table.put(p);

  Get g = new Get(Bytes.toBytes("row1"));
  Result r = table.get(g);

  byte [] value = r.getValue(Bytes.toBytes("Id"),Bytes.toBytes("col1"));
  byte [] value1 = r.getValue(Bytes.toBytes("Name"),Bytes.toBytes("col2"));

  String valueStr = Bytes.toString(value);
  String valueStr1 = Bytes.toString(value1);
  System.out.println("GET: " +"Id: "+ valueStr+"Name: "+valueStr1);


  Scan s = new Scan();
  s.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("col1"));
  s.addColumn(Bytes.toBytes("Name"), Bytes.toBytes("col2"));
  ResultScanner scanner = table.getScanner(s);

  try
  {
     for (Result rnext = scanner.next(); rnext != null; rnext = scanner.next())
     {
        System.out.println("Found row : " + rnext);
     }
  }
  finally
    {
       scanner.close();
    }


  }


}



