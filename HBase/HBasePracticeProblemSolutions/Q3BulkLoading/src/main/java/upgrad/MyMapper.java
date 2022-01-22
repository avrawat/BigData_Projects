package upgrad;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

	ImmutableBytesWritable hkey = new ImmutableBytesWritable();
	KeyValue kv;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] vals = null;

		try {
			vals = value.toString().split(";");
			
			hkey.set(Bytes.toBytes(vals[0]));
			kv = new KeyValue(hkey.get(), Bytes.toBytes("M"), Bytes.toBytes("name"), Bytes.toBytes(vals[1]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("M"), Bytes.toBytes("year"), Bytes.toBytes(vals[2]));
			context.write(hkey, kv);
			
		} catch (Exception e) {
			System.out.println("\nException in map function\n");
			System.out.println(e.getMessage());
		}


	}
}