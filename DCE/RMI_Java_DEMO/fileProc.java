
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class fileProc extends UnicastRemoteObject implements calcInterface
{

	private Map<String,String> data = null; 
	private static final long serialVersionUID = 1L;

	public fileProc() throws RemoteException{
		super();
	}


	public void loadFile(String path) throws RemoteException {

		System.out.println("Reading the file");
		String fileName = path;
		BufferedReader reader;
		data = new HashMap<String, String>();

		try {
			reader = new BufferedReader(new FileReader(fileName));
			String line = reader.readLine(); // the header, to be ignored

			while (line != null) {
				String[] vals = line.split(",");
				String value = vals[1] + "," + vals[15] + "," + vals[16] + "," + vals[22];
				data.put(vals[0], value);
				line = reader.readLine();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("The count is " + data.size());
	}

	public int[] sum() throws RemoteException {

		int[] arr = { 0, 0, 0, 0, 0 };

		Iterator<String> values = data.values().iterator();
		while (values.hasNext()) {
			String[] vals = values.next().split(",");
			if (!(vals[1].equals("NA") || vals[2].equals("NA") || vals[3].equals("NA"))) {
				int totalDelay = Integer.parseInt(vals[1]) + Integer.parseInt(vals[2]);

				if (totalDelay > 0) {
					if (vals[0].equals("2004")) {
						arr[0] += totalDelay;
					} else if (vals[0].equals("2005")) {
						arr[1] += totalDelay;
					} else if (vals[0].equals("2006")) {
						arr[2] += totalDelay;
					} else if (vals[0].equals("2007")) {
						arr[3] += totalDelay;
					} else if (vals[0].equals("2008")) {
						arr[4] += totalDelay;
					}
				}
			}
		}

		return arr;
	}

	public int[] avg() throws RemoteException{
		int[] count = { 0, 0, 0, 0, 0 };
		int[] avg = { 0, 0, 0, 0, 0 };
		int[] totalDelay = this.sum();

		Iterator<String> values = data.values().iterator();

		while (values.hasNext()) {
			String[] vals = values.next().split(",");
			if (!(vals[1].equals("NA") || vals[2].equals("NA") || vals[3].equals("NA"))) {
				if (vals[0].equals("2004")) {
					count[0] += 1;
				} else if (vals[0].equals("2005")) {
					count[1] += 1;
				} else if (vals[0].equals("2006")) {
					count[2] += 1;
				} else if (vals[0].equals("2007")) {
					count[3] += 1;
				} else if (vals[0].equals("2008")) {
					count[4] += 1;
				}
			}
		}
		for (int i = 0; i < 5; i++) {
			avg[i] = totalDelay[i] / count[i];
		}
		return avg;
	}

	public int[] count() throws RemoteException{
		int[] count = { 0, 0, 0, 0, 0 };

		Iterator<String> values = data.values().iterator();
		while (values.hasNext()) {
			String[] vals = values.next().split(",");
			if ( vals[0].equals("2004") && vals[3].equals("1")) {
				count[0] += 1;
			} else if (vals[0].equals("2005") && vals[3].equals("1")) {
				count[1] += 1;
			} else if (vals[0].equals("2006") && vals[3].equals("1")) {
				count[2] += 1;
			} else if (vals[0].equals("2007") && vals[3].equals("1")) {
				count[3] += 1;
			} else if (vals[0].equals("2008") && vals[3].equals("1")) {
				count[4] += 1;
			}
		}

		return count;
	}

}
	