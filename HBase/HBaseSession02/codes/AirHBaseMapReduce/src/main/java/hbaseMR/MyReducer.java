package hbaseMR;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
	
	// target Table
	// column family
	public static final byte[] CF = Bytes.toBytes("Delay");
	// column name
	public static final byte[] ATTR1 = Bytes.toBytes("avgDelay");

	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
	int sum = 0,count=0;
	
		for(IntWritable val: values){
			sum += val.get();
			count++;
		}
		
		int avg = sum/count;
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn(CF, ATTR1, Bytes.toBytes(Integer.toString(avg)));
		context.write(null,put);
	}
}