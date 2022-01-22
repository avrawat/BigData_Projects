package com.upgrad.mapjoin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private String schemaFile = null;

	private Properties prop = new Properties();
	private InputStream input = null;
	private String seqNo = null;
	private Map<String, String> mapping = new HashMap<String, String>();

	@Override

	public void setup(Context context) throws IOException {
		URI[] uris = context.getCacheFiles();
		schemaFile = uris[0].toString();
		try {

			input = new FileInputStream(schemaFile);
			// load a properties file
			prop.load(input);
			// cardType = prop.getProperty("1");
			for (Map.Entry<Object, Object> e : prop.entrySet()) {
				String key = (String) e.getKey();
				String value = (String) e.getValue();
				mapping.put(key, value);

			}

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] columns = value.toString().split(",");
		String newJoinedData = "";

		for (int i = 0; i < (columns.length); i++) {
			if (newJoinedData.equalsIgnoreCase(""))
				newJoinedData = columns[i];
			else
				newJoinedData = newJoinedData + "\t" + columns[i];
		}
		seqNo = mapping.get(columns[1]);
		newJoinedData = newJoinedData + "\t" + seqNo;
		context.write(new Text(newJoinedData), NullWritable.get());

	}

}
