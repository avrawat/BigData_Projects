import java.rmi.*;

public class server{
	

	public static void main(String[] args) throws RemoteException{

		// public IP of the server
		String serverIP = "192.168.9.117";
		// port number
		String serverPort = "1997";
		//  name of the RMI Redistry
		String bname = "JavaRMI";

		// Server location - distributed
		String bindLocation = "rmi://" + serverIP + ":" + serverPort + "/" + bname;

		// Server location - local
		//String bindLocation = "rmi://localhost:1997/JavaRMI";

		// Create the object
		calc obj  = new calc();
		
		try{
			// bind the Calc object
			Naming.bind(bindLocation,obj);
		}catch(Exception e){
			System.out.println("Server: Error while binding the object");
			e.printStackTrace();
		}

		System.out.println("Server is ready at:" + bindLocation);
	}

}