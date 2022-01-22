
import java.rmi.*;
public class client{


	public static void main(String[] args) throws Exception
	{

		// public IP of the server
		String serverIP = "192.168.1.105";
		// port number
		String serverPort = "1997";
		//  name of the RMI Redistry
		String bname = "JavaRMI";

		// Server location - Distributed
		String connectLocation = "rmi://" + serverIP + ":" + serverPort + "/" + bname;

		// Server location- local
		//String connectLocation = "rmi://localhost:1997/JavaRMI";

		// lookup for the Calc object, in the server rmi registry
		calcInterface obj = (calcInterface)Naming.lookup(connectLocation);

		// invoke the remote procedures
		int a = obj.add(6,4);
		int s = obj.subtract(18,8);
		int d = obj.divide(50,5);
		int m = obj.multiply(5,2);
		
		int total = a + s + d + m;
		//print the reults
		System.out.println("The total is "+total);
		System.out.println("Terminating Client Program...");
	}
}