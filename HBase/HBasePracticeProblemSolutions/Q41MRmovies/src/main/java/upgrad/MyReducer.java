package upgrad;

import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

public class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

	public static final byte[] CF = "R".getBytes();
	public static final byte[] COUNT = "count".getBytes();

	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		
	int i = 0;
		for(IntWritable val: values){
			i+= val.get();
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn(CF, COUNT, Bytes.toBytes(Integer.toString(i)));
		context.write(null,put);
	}
}