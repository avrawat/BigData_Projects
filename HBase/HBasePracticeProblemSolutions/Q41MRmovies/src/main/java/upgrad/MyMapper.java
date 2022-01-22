package upgrad;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class MyMapper extends TableMapper<Text, IntWritable>{
	public static final byte[] CF = "M".getBytes(); 	
	public static final byte[] ATTR2 = "year".getBytes();
	private final IntWritable ONE = new IntWritable(1);
	private Text text = new Text();
 

 	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException{
 		
 		
 		String val = new String(value.getValue(CF,ATTR2));
 		text.set(val);
 		context.write(text, ONE);

 	}
}