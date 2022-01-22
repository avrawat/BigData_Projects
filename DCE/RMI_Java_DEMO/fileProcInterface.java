import java.rmi.Remote;
import java.rmi.RemoteException;

public interface fileProcInterface extends Remote{

	public void loadFile(String path) throws RemoteException;
	public int[] sum() throws RemoteException;
	public int[] avg () throws RemoteException;
	public int[] count() throws RemoteException;
}