package udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class MyReducer extends EvalFunc<Tuple> {

	public Tuple exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null;
		}

		Object word = input.get(0);
		long count = 1;
		
		if (input.get(1) instanceof DataBag) {
			count = ((DataBag) input.get(1)).size();
		}

		Tuple tuple = TupleFactory.getInstance().newTuple();
		tuple.append(word);
		tuple.append(count);
		return tuple;
	}

}
