package upgrad;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class MyMapper2 extends TableMapper<IntWritable, Text>{
	public static final byte[] CF = "R".getBytes(); 	
	public static final byte[] ATTR2 = "count".getBytes();
	
	private final IntWritable ONE = new IntWritable(1);
	private Text text = new Text();
 

 	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException{
 		
 		String year=new String(value.getRow());
 		
 		String count = new String(value.getValue(CF,ATTR2));
 		
 		text.set(count);
 		context.write(new IntWritable(Integer.MAX_VALUE-Integer.parseInt(count)), new Text(year));

 	}
}