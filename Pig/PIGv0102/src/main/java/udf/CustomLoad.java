package udf;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class CustomLoad extends LoadFunc {
	protected RecordReader in = null;
	private byte fieldDel = '\t';
	private ArrayList<Object> mProtoTuple = null;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();
	

	
	public CustomLoad(String delimiter) {
	
		 if (delimiter.length() == 1) {
	            this.fieldDel = (byte)delimiter.charAt(0);
	            
	        } else if (delimiter.length() >  1 && delimiter.charAt(0) == '\\') {
	            switch (delimiter.charAt(1)) {
	                case 't':
	                    this.fieldDel = (byte)'\t';
	                    break;

	                case 'x':
	                    fieldDel =
	                            Integer.valueOf(delimiter.substring(2), 16).byteValue();
	                    break;

	                case 'u':
	                    this.fieldDel =
	                            Integer.valueOf(delimiter.substring(2)).byteValue();
	                    break;

	                default:
	                    throw new RuntimeException("Unknown delimiter " + delimiter);
	            }
	        } else {
	            throw new RuntimeException("PigStorage delimeter must be a single character");
	        }
	}// constructor
	
	// tells where the data resides
	// can be called multiple times
	@Override
	public void setLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		FileInputFormat.setInputPaths(job, location);
	}

	// Tells the loader the Hadoop input format, called only once
	// can be called multiple times
	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return new TextInputFormat();
	}

	// involed just before the loading begins
	// called once
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
		// TODO Auto-generated method stub
		in = reader;
	}

	// returns tuple for corresponding to each row which is generated.
	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		Tuple t=null;
		
		try {
			boolean notDone = in.nextKeyValue();
			if (!notDone) {
				return null;
			}
			

			Text value = (Text) in.getCurrentValue();
	
			byte[] buf = value.getBytes();
			int len = value.getLength();
			int start = 0;

			for (int i = 0; i < len; i++) {
				if (buf[i] == fieldDel) {
					readField(buf, start, i);
					start = i + 1;
				}
			}

			readField(buf, start, len);

			t = mTupleFactory.newTupleNoCopy(mProtoTuple);
			mProtoTuple = null;
			

		} catch (Exception e) {
			int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
		}
		return t;
	}

	private void readField(byte[] buf, int start, int end) {
		
		if (mProtoTuple == null) {
			mProtoTuple = new ArrayList<Object>();
		}
		if (start == end) {
			mProtoTuple.add(null);
		} else {
			mProtoTuple.add(new DataByteArray(buf, start, end));

		}
	}

}// main tuple
