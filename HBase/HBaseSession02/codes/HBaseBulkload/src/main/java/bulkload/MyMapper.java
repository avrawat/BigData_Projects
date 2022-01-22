package bulkload;


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
	protected void setup(Context context) throws IOException, InterruptedException {}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] vals = null;

		try {
			
			//csv file
			vals = value.toString().split(",");
			
			//ignore header
			if(vals[0].equals("ID")) {
				return;
			}
			
			//row key
			hkey.set(Bytes.toBytes(vals[0]));
			/**********************************************************************************************************/
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("year"), Bytes.toBytes(vals[1]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("month"), Bytes.toBytes(vals[2]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("dayofMonth"), Bytes.toBytes(vals[3]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("dayofWeek"), Bytes.toBytes(vals[4]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("depTime"), Bytes.toBytes(vals[5]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("schDepTime"), Bytes.toBytes(vals[6]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("arrTime"), Bytes.toBytes(vals[7]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("schArrTime"), Bytes.toBytes(vals[8]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("arrDelay"), Bytes.toBytes(vals[15]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("time"), Bytes.toBytes("depDelay"), Bytes.toBytes(vals[16]));
			context.write(hkey, kv);
			/**********************************************************************************************************/
			
			/**********************************************************************************************************/
			kv = new KeyValue(hkey.get(), Bytes.toBytes("flight"), Bytes.toBytes("uniqCarrier"), Bytes.toBytes(vals[9]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("flight"), Bytes.toBytes("flightNum"), Bytes.toBytes(vals[10]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("flight"), Bytes.toBytes("tailNum"), Bytes.toBytes(vals[11]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("flight"), Bytes.toBytes("origin"), Bytes.toBytes(vals[17]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("flight"), Bytes.toBytes("dest"), Bytes.toBytes(vals[18]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("flight"), Bytes.toBytes("distance"), Bytes.toBytes(vals[19]));
			context.write(hkey, kv);
			/**********************************************************************************************************/
			
			/**********************************************************************************************************/
			kv = new KeyValue(hkey.get(), Bytes.toBytes("status"), Bytes.toBytes("cancelled"), Bytes.toBytes(vals[22]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("status"), Bytes.toBytes("canCode"), Bytes.toBytes(vals[23]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("status"), Bytes.toBytes("diverted"), Bytes.toBytes(vals[24]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("status"), Bytes.toBytes("carrDelay"), Bytes.toBytes(vals[25]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("status"), Bytes.toBytes("weatherDelay"), Bytes.toBytes(vals[26]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("status"), Bytes.toBytes("nasDelay"), Bytes.toBytes(vals[27]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("status"), Bytes.toBytes("securityDelay"), Bytes.toBytes(vals[28]));
			context.write(hkey, kv);
			kv = new KeyValue(hkey.get(), Bytes.toBytes("status"), Bytes.toBytes("lateAircraftDelay"), Bytes.toBytes(vals[29]));
			context.write(hkey, kv);
			/**********************************************************************************************************/
			
				
		} catch (Exception e) {
			System.out.println("\nException in map function\n");
			System.out.println(e.getMessage());
		}


	}
}
