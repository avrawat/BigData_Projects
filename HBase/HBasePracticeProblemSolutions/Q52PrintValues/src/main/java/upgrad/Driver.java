package upgrad;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Driver {

	public static void main(String[] args) throws IOException {

		String table = args[0];
		Logger.getRootLogger().setLevel(Level.WARN);

		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the server...");
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		System.out.println("Connected");

		if (admin.tableExists(TableName.valueOf(table))) {

			Table htable = con.getTable(TableName.valueOf(table));

			Scan scan = new Scan();
			ResultScanner scanner = htable.getScanner(scan);

			System.out.println("---------------------------");

			for (Result res : scanner) {

				for (Cell cell : res.listCells()) {

					byte[] arr = cell.getRowArray();

					System.out.print(
							Bytes.toString(Arrays.copyOfRange(arr, cell.getRowOffset(), cell.getFamilyOffset() - 1)));

					System.out.print(" " + Bytes
							.toString(Arrays.copyOfRange(arr, cell.getFamilyOffset(), cell.getQualifierOffset())));

					System.out.print(" " + Bytes.toString(Arrays.copyOfRange(arr, cell.getQualifierOffset(),
							cell.getQualifierLength() + cell.getQualifierOffset())));

					Timestamp ts = new Timestamp(cell.getTimestamp());
					Date date = new Date(ts.getTime());
					System.out.print(" " + date);

					System.out.print(" "
							+ Bytes.toString(Arrays.copyOfRange(arr, arr.length - cell.getValueLength(), arr.length)));

					System.out.println();

				}
			} // loop

		} else {

			System.out.println("The HBase Table named " + table + " doesn't exists.");
		}

		System.out.println("Exiting Program");

	}//main

}
