package udf;

import java.util.*;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;

public class DistributeRevenue extends EvalFunc<DataBag> {
	public DataBag exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0 || input.get(0) == null)
			return null;

		float top = 0, side = 0;

		// get the data bags
		DataBag result = (DataBag) input.get(0);
		DataBag revenue = (DataBag) input.get(1);

		DataBag db = new DefaultDataBag();

		long size = result.size();

		for (Tuple t : revenue) {
			String s = t.get(1).toString();
			if (s.equals("top")) {
				top = (Float) t.get(2);
			} else if (s.equals("side")) {
				side = (Float) t.get(2);
			}
		}

		float average = side / size;

		TupleFactory tf = TupleFactory.getInstance();

		for (Tuple tuple : result) {

			int rank = (Integer) tuple.get(2);
			String query = tuple.get(1).toString();

			List<String> ls = new ArrayList<String>();
			if (rank == 1) {
				ls.add(query);
				ls.add(Float.toString(top + average));
				db.add(tf.newTuple(ls));
			} else {
				ls.add(query);
				ls.add(Float.toString(average));
				db.add(tf.newTuple(ls));
			}
		}
		return db;
	}
}
