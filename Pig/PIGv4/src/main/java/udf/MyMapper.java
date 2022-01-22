package udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class MyMapper extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple input) throws IOException {

		Tuple tuple = null;
		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null;
		}

		ArrayList<Tuple> list = new ArrayList<Tuple>();
		String line = input.get(0).toString();

		for (String word : line.split("[^0-9aA-zZ]")) {
			tuple = TupleFactory.getInstance().newTuple();
			tuple.append(1);
			tuple.append((Object) word);
			list.add(tuple);
		}
		DataBag bag = BagFactory.getInstance().newDefaultBag(list);
		return bag;
	}

}
