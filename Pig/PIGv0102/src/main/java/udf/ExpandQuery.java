package udf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ExpandQuery extends EvalFunc<DataBag> {
	
	
	private HashMap<String, String> map = null;
	@Override
	public DataBag exec(Tuple input) throws IOException {
		
		if(input==null || input.size()==0) {
			return null;
		}
		
		createMap();
		ArrayList<Tuple> list = new ArrayList<Tuple>();
		Tuple tuple = null;
	
		String val = input.get(0).toString();
		
		// if the key is not available in the map
		if(!map.containsKey(val)) {
			tuple = TupleFactory.getInstance().newTuple();
			tuple.append((Object)val + " NA");
			list.add(tuple);
			DataBag bag = BagFactory.getInstance().newDefaultBag(list);
			return bag;
		}
		
		String keywords = map.get(val);
		for(String v : keywords.split(",")){	
			tuple = TupleFactory.getInstance().newTuple();
			tuple.append((Object)( val +" "+v));
			list.add(tuple);
		}
		
		DataBag bag = BagFactory.getInstance().newDefaultBag(list);
		return bag;

	}

	public void createMap() {
		
		// Assume this map is given to us
		// this map contains most frequently searched keywords[value] for a query string[key]
	    map = new HashMap<String, String>();
			map.put("Samsung", "mobile,J6");
			map.put("Dell","PC,Inspiron,Laptops");
			map.put("OnePlus","6,5T,3,Sale");
			map.put("Lava", "Company,Loss,why");
			map.put("Apple","iMac,iPad,iPhone,iGobar");
			map.put("MI","sale,speakers,phones");
			map.put("Google","Pixel,Maps,translate");
			map.put("HP","gas,sale,Laptop");
			map.put("Lenovo","z8,k8 Note,laptop,sale,thinkpad");
			map.put("LG","fridge,TV,mobile");
			map.put("Micromax","canvas,bharat,sale");
			map.put("BenQ","PC,monitor");
			map.put("Asus","zenphone,laptop sale");
			map.put("Nokia", "3310,Asha,6.1,7.1");
	}

}











