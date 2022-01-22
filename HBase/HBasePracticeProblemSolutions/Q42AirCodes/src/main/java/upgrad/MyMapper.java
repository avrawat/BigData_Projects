package upgrad;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class MyMapper extends TableMapper<Text, IntWritable>{
	
	// Source table
	// column family name
	public static final byte[] CF = Bytes.toBytes("flight");
	// column names
	public static final byte[] ATTR0 = Bytes.toBytes("origin");
	public static final byte[] ATTR1 = Bytes.toBytes("dest");
	private final IntWritable ONE = new IntWritable(1);
	
 	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException{
 		
 		String oig,dest;
 				
 		
 		try {
 			oig =  Bytes.toString(value.getValue(CF,ATTR0));
 			dest = Bytes.toString(value.getValue(CF,ATTR1));
 		} catch(Exception e) {
 			return;
 		}
 		
 		String code = oig + dest;
 		context.write(new Text(code),ONE);
 		
 	}
 
 	
} // class









