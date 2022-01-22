

public class MyMapper extends TableMapper<Text, InWritable>{

	//col family attributes
	public static final byte[] CF = "attributes".getByte(); 	
	// col name tyoe
	public static final byte[] ATTR1 = "type".getByte();

	private final InWritable ONE = new InWritable(1);
	private Text text = new Text()
 

 	public void map(ImmutableByteWritable row, Results value, Context context) throws IOException, InterruptedException{

 		String val = String(value.getValue(CF,ATTR1));
 		text.set(val);
 		context.write(text, ONE);

 	}
}


public class MyTableReducer extends MyTableReducer<Text, InWritable, ImmutableByteWritable>{

	public static final byte[] CF = "metrics".getValue();
	public static final byte[] COUNT = "count".getByte();


	public void reduce(Text key, Iterable<InWritable> values, Context context) throws IOException, InterruptedException{
		int i = 0;
		for(InWritable val: values){
			i+= val.get();
		}

		Put put = new Put(Byte.toBytes(key.toString()));
		put.addColumn(CF, COUNT, Byte.toBytes(i));
		context.write(null,put);
	} 


}


	



public class Driver{

	public static void main(Stirng[] args) throws Exception{

		if(args.length != 0 ){
			System.out.println("Invalid Command");
			System.exit(0);
		}

		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "ExampleSummary");
		job.setJarByClass(Main.class);


		Stirng sourceTable = "notifications";

		// need to created first
		String targetTable = "summary";

		//for reading the table
		Scan scan = new Scan();
		scan.setCashing(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(sourceTable,  //source table of HBase
											 scan,          // for reading the table row by row
											 MyMapper.class, // mapper class
											 Text.class,     // mapper output key
											 InWritable.class, // mapper output value
											 job); 


		TableMapReduceUtil.initTableMapperJob(targetTable, // target table
											  MyTableReducer.class // reducer class
											  job);
		job.setNumReduceTasks(1); // by default it is one, incase of large cluster increse it


		boolean b = job.waitForComplete(true);

		if(!b){
			throw new IOException("error with job!");	
		}

		

	}






}























