package upgrad;

import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

public class MyReducer2 extends TableReducer<IntWritable, Text, ImmutableBytesWritable> {

	public static final byte[] CF = "R".getBytes();
	public static final byte[] COUNT = "count".getBytes();
	public static int r=0;
	
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		if(r<4)
		{
		for(Text val: values){
			r++;
			Put put = new Put(Bytes.toBytes(val.toString()));
			put.addColumn(CF, COUNT, Bytes.toBytes(Integer.toString(Integer.MAX_VALUE - key.get())));
			context.write(null,put);
			if(r==4)
				break;
		}
		}
	}
}