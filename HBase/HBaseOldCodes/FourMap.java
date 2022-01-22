package hbasetest;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class FourMap {
	Map<FourTuple,String> Table= new TreeMap<FourTuple,String>();
	
	
	public void put(String rk, String cf, String cn, int timestamp, String value) {
		
		Table.put(new FourTuple(rk,cf,cn,timestamp),value);
		
	}
	
	
	public void scan() {
		
		for(Map.Entry<FourTuple, String> entry : Table.entrySet()) {
			FourTuple key = entry.getKey();
				System.out.print(key.rk.toString()+" ");
				System.out.print(key.cf.toString()+" ");
				System.out.print(key.cn.toString()+" ");
				System.out.print(key.timestamp.toString()+" ");				
				System.out.println(entry.getValue());
		}
	}
	

	
	public void get(String rowkey) {

		System.out.println("********** key lookup ************");
		
		for(Map.Entry<FourTuple, String> entry : Table.entrySet()) {
			FourTuple key = entry.getKey();
			if(key.rk.toString().equals(rowkey)){
				System.out.print(key.rk.toString()+" ");
				System.out.print(key.cf.toString()+" ");
				System.out.print(key.cn.toString()+" ");
				System.out.print(key.timestamp.toString()+" ");				
				System.out.println(entry.getValue());
			}
		}
		
	}// row key
	
	
	public void get(String rowkey, String cf) {

		System.out.println("********** key, cf lookup ************");
		
		for(Map.Entry<FourTuple, String> entry : Table.entrySet()) {
			FourTuple key = entry.getKey();
			if(key.rk.toString().equals(rowkey) && key.cf.toString().equals(cf)){
				System.out.print(key.rk.toString()+" ");
				System.out.print(key.cf.toString()+" ");
				System.out.print(key.cn.toString()+" ");
				System.out.print(key.timestamp.toString()+" ");				
				System.out.println(entry.getValue());
			}
		}
		
	} // cf
	
	
	public void get(String rowkey, String cf, String cn) {

		System.out.println("********** key,cf,cn lookup ************");
		
		for(Map.Entry<FourTuple, String> entry : Table.entrySet()) {
			FourTuple key = entry.getKey();
			if(key.rk.toString().equals(rowkey) && key.cf.toString().equals(cf) && key.cn.toString().equals(cn)){
				System.out.print(key.rk.toString()+" ");
				System.out.print(key.cf.toString()+" ");
				System.out.print(key.cn.toString()+" ");
				System.out.print(key.timestamp.toString()+" ");				
				System.out.println(entry.getValue());
			}
		}
		
	}// cn
	
		
	public void get(String rowkey, String cf, String cn, String timeStamp) {

		System.out.println("********** key,cf,cn,ts lookup ************");
		
		FourTuple key = new FourTuple(rowkey,cf,cn,timeStamp);
		
	   String value = Table.get(key);

		System.out.print(key.rk.toString()+" ");
		System.out.print(key.cf.toString()+" ");
		System.out.print(key.cn.toString()+" ");
		System.out.print(key.timestamp.toString()+" ");				
		System.out.println(value);
	   	
		
	}// cn
	
		
	
	
	
	
	public static void main(String args[]) {
		FourMap fm = new FourMap();
		int timestamp=0;
		fm.put("com.upgrad.www", "links", "top", timestamp++,"www.upgrad.com/big-data");
		fm.put("com.upgrad.www", "links", "side",timestamp++, "www.upgrad.com/data-science");
		fm.put("com.upgrad.www", "links", "top", timestamp++,"www.upgrad.com/big-data-engineering");
		
		
		fm.put("com.google.www", "links", "top", timestamp++,"images.google.com");
		fm.put("com.google.www", "links", "top", timestamp++,"drive.google.com");
		fm.put("com.google.www", "links", "mid", timestamp++,"trends.google.com");
		fm.put("com.google.www", "links", "mid", timestamp++,"cloud.google.com");
		fm.put("com.google.www", "links", "bottom", timestamp++,"docs.google.com");
		fm.put("com.google.www", "links", "bottom", timestamp++,"sheets.google.com");
		

		
		fm.put("com.google.docs","Text", "top", timestamp++, "Welcome to Google Drive");
		fm.put("com.google.docs","Text", "bottom", timestamp++, "Go to next page");
		fm.put("com.google.docs","Files", "owned", timestamp++, "docs.google.com/file1");
		fm.put("com.google.docs","Files", "shared", timestamp++, "docs.google.com/file2");
		
		
		fm.scan();
		
		fm.get("com.upgrad.www");
		fm.get("com.google.docs","Files");
		fm.get("com.google.docs","Files","owned");
		fm.get("com.google.docs","Files","owned","12");
		
		
		
		
		
	}
	
	
	
	

}








