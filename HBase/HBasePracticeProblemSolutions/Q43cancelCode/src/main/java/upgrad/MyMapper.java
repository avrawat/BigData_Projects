package upgrad;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class MyMapper extends TableMapper<Text, IntWritable> {

	// Source table
	// column family name
	public static final byte[] CF = Bytes.toBytes("status");
	// column names
	public static final byte[] ATTR0 = Bytes.toBytes("canCode");
	private final IntWritable ONE = new IntWritable(1);

	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {

		String code;
		String key = "total";

		try {
			code = Bytes.toString(value.getValue(CF, ATTR0));
		} catch (Exception e) {
			return;
		}

		if (code.isEmpty()) {
			return;
		}

		if (code.equals("A")) {
			context.write(new Text(code), ONE);
			context.write(new Text(key), ONE);
		} else if (code.equals("B")) {
			context.write(new Text(code), ONE);
			context.write(new Text(key), ONE);
		} else if (code.equals("C")) {
			context.write(new Text(code), ONE);
			context.write(new Text(key), ONE);
		} else if (code.equals("D")) {
			context.write(new Text(code), ONE);
			context.write(new Text(key), ONE);
		}

	}

} // class
