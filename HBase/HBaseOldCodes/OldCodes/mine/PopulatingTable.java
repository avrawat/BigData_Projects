
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.habse.util.*;





public class PopulatingTable{

	public static void main(String args[]){


		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "RawatKiTable");


		/*********** adding a new row ***********/

		// adding a row key

		Put p = new Put(Bytes.toBytes("row1"));

		p.add(Bytes.toBytes("ContactDetails"), Bytes.toBytes("Mobile"), Bytes.toBytes("9876543210"));
		p.add(Bytes.toBytes("ContactDetails"), Bytes.toBytes("Email"), Bytes.toBytes("abhc@gmail.com"));

		p.add(Bytes.toBytes("Personal"), Bytes.toBytes("Name"), Bytes.toBytes("Abhinav Rawat"));
		p.add(Bytes.toBytes("Personal"), Bytes.toBytes("Age"), Bytes.toBytes("21"));
		p.add(Bytes.toBytes("Personal"), Bytes.toBytes("Gender"), Bytes.toBytes("M"));

		p.add(Bytes.toBytes("Employement"), Bytes.toBytes("Company"), Bytes.toBytes("UpGrad"));
		p.add(Bytes.toBytes("Employement"), Bytes.toBytes("DOJ"), Bytes.toBytes("11:06:2018"));
		p.add(Bytes.toBytes("Employement"), Bytes.toBytes("Designation"), Bytes.toBytes("ContentStrategist"));

		table.Put(p);


		/**********************/



		p = new Put(Bytes.toBytes("row2"));

		p.add(Bytes.toBytes("ContactDetails"), Bytes.toBytes("Mobile"), Bytes.toBytes("1234567890"));
		p.add(Bytes.toBytes("ContactDetails"), Bytes.toBytes("Email"), Bytes.toBytes("abc@gmail.com"));

		p.add(Bytes.toBytes("Personal"), Bytes.toBytes("Name"), Bytes.toBytes("Tony Stark"));
		p.add(Bytes.toBytes("Personal"), Bytes.toBytes("Age"), Bytes.toBytes("45"));
		p.add(Bytes.toBytes("Personal"), Bytes.toBytes("Gender"), Bytes.toBytes("M"));

		p.add(Bytes.toBytes("Employement"), Bytes.toBytes("Company"), Bytes.toBytes("Stark Industries"));
		p.add(Bytes.toBytes("Employement"), Bytes.toBytes("DOJ"), Bytes.toBytes("05:05:2008"));
		p.add(Bytes.toBytes("Employement"), Bytes.toBytes("Designation"), Bytes.toBytes("Founder"));

		table.Put(p);


		/**********************/




		p = new Put(Bytes.toBytes("row3"));

		p.add(Bytes.toBytes("ContactDetails"), Bytes.toBytes("Mobile"), Bytes.toBytes("9988776600"));
		p.add(Bytes.toBytes("ContactDetails"), Bytes.toBytes("Email"), Bytes.toBytes("xyz@gmail.com"));

		p.add(Bytes.toBytes("Personal"), Bytes.toBytes("Name"), Bytes.toBytes("Steve Rogers"));
		p.add(Bytes.toBytes("Personal"), Bytes.toBytes("Age"), Bytes.toBytes("90"));
		p.add(Bytes.toBytes("Personal"), Bytes.toBytes("Gender"), Bytes.toBytes('M'));

		p.add(Bytes.toBytes("Employement"), Bytes.toBytes("Company"), Bytes.toBytes("Avengers"));
		p.add(Bytes.toBytes("Employement"), Bytes.toBytes("DOJ"), Bytes.toBytes("05:05:2011"));
		p.add(Bytes.toBytes("Employement"), Bytes.toBytes("Designation"), Bytes.toBytes("Captain"));

		table.Put(p);


		/**********************/


		System.out.print("Table is Populated");


		Get g = new Get(Bytes.toBytes("row1"));
		Results r = table.get(g);

		byte [] value1  = r.getValue(Bytes.toBytes("Personal"),Bytes.toBytes("Name"));
		byte [] value2  = r.getValue(Bytes.toBytes("Personal"), Bytes.toBytes("Age"));
		byte [] value3  = r.getValue(Bytes.toBytes("Personal"), Bytes.toBytes("Genger"));

		System.out.print("*************************************************");
		System.out.print("row1 "+Bytes.toString(value1) + " " + Bytes.toInteger(value2) + " " + Bytes.toCharacter(value3));



		Get g = new Get(Bytes.toBytes("row2"));
		Results r = table.get(g);

		byte [] value1  = r.getValue(Bytes.toBytes("Personal"),Bytes.toBytes("Name"));
		byte [] value2  = r.getValue(Bytes.toBytes("Personal"), Bytes.toBytes("Age"));
		byte [] value3  = r.getValue(Bytes.toBytes("Personal"), Bytes.toBytes("Genger"));

		System.out.print("*************************************************");
		System.out.print("row2 "+Bytes.toString(value1) + " " + Bytes.toInteger(value2) + " " + Bytes.toCharacter(value3));


		System.out.println("The scanner will print now....");


	  	Scan s = new Scan();
	  	s.addColumn(Bytes.toBytes("ContactDetails"), Bytes.toBytes("Mobile"));
	  	s.addColumn(Bytes.toBytes("ContactDetails"), Bytes.toBytes("Email"));
	  	ResultScanner scanner = table.getScanner(s);

		  try
		  {
		     for (Result rnext = scanner.next(); rnext != null; rnext = scanner.next())
		     {
		        System.out.println("Found row : " + rnext);
		     }
		  }


		  finally
		    {
		       scanner.close();
		    }

			
			table.close();

		    System.out.println("The Program Returned....");



	}





}