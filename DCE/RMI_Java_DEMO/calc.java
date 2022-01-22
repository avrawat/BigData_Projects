
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class calc extends UnicastRemoteObject implements calcInterface
{

	private static final long serialVersionUID = 1L;

	public calc() throws RemoteException{
		super();
	}

	public int add(int x,int y){
		System.out.println("Remote Add method is envoked...");
		return x + y;
	}

	public int subtract (int x, int y){
		System.out.println("Remote Subtract method is envoked...");
		return x - y;

	}

	public int divide(int x, int y){
		System.out.println("Remote Divide method is envoked...");
		return (int) x / y;
	}

	public int multiply(int x, int y){
		System.out.println("Remote Multiply method is envoked...");
		return x * y;

	}

}
	