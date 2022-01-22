package hbaseMR;


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
	public static final byte[] CF = Bytes.toBytes("time");
	// column names
	public static final byte[] ATTR0 = Bytes.toBytes("year");
	public static final byte[] ATTR1 = Bytes.toBytes("arrDelay");
	public static final byte[] ATTR2 = Bytes.toBytes("depDelay");
	
	
 	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException{
 		
 		String arr,dep;
 		
 		
 		try {
 		 arr = Bytes.toString(value.getValue(CF,ATTR1));
 		 dep = Bytes.toString(value.getValue(CF,ATTR2));
 		} catch(Exception e) {
 			return;
 		}
 		
 		String year = Bytes.toString((value.getValue(CF,ATTR0)));
 		
 		 		
 		if(isInteger(arr) && isInteger(dep)) {
 			int sum = Integer.parseInt(arr)+Integer.parseInt(dep);
 			if(sum > 0)
 				context.write(new Text(year),new IntWritable(sum));
 			else
 				context.write(new Text(year), new IntWritable(0));
 		}
 		  
 	}
 	
 	public boolean isInteger(String str) {
 		
 	   if (str == null) {
	        return false;
	    }
 	   
 		int length = str.length();
 		
 	 
 	    if (str.isEmpty()) {
 	        return false;
 	    }
 	    int i = 0;
 	    
 	    if (str.charAt(0) == '-') {
 	        if (length == 1) {
 	            return false;
 	        }
 	        i = 1;
 	    }
 	    for (; i < length; i++) {
 	        char c = str.charAt(i);
 	        if (c < '0' || c > '9') {
 	            return false;
 	        }
 	    }
 	    return true;
 	}
 	
 	
} // class









