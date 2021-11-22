package com.upgrad.FraudAnalysis;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseLookUpTableWriteDAO {


	@SuppressWarnings("deprecation")
	public static void putData(Transaction transactionData,String awsUrl,String clientPort,String masterPort) throws IOException {
		@SuppressWarnings("deprecation")
		Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin(awsUrl,clientPort,masterPort);
		HTable table = null;
		Transaction transactionResult = null;
		try {
			transactionResult = new Transaction();
			/*String cardID = transactionData.getCard_id() + "";
			String postcode = transactionData.getPostcode() + "";
			String transaction_dt = transactionData.getTransaction_dt() + "";
			*/
			HTable hTable = new HTable(hBaseAdmin1.getConfiguration(), "look_up_hive");
			//String rowkey = transactionData.getCard_id() + "";
			Put p = new Put(Bytes.toBytes(transactionData.getCard_id() + ""));
			p.add(Bytes.toBytes("lookup"), Bytes.toBytes("postcode"),Bytes.toBytes(transactionData.getPostcode() + ""));
			p.add(Bytes.toBytes("lookup"), Bytes.toBytes("transaction_dt"),Bytes.toBytes(transactionData.getTransaction_dt() + ""));
			hTable.put(p); 
			//System.out.println("data inserted");
		} catch (

		Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null)
					table.close();
				hBaseAdmin1.getConnection().close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}


}
