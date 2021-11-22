package com.upgrad.FraudAnalysis;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTransactionTableWriteDAO {

	@SuppressWarnings("deprecation")
	public static void putData(Transaction transactionData,String awsUrl,String clientPort,String masterPort) throws IOException {
		@SuppressWarnings("deprecation")
		// public static Transaction getScore(String transactionData) throws IOException
		// {
		Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin(awsUrl,clientPort,masterPort);
		HTable table = null;
		Transaction transactionResult = null;
		try {
			transactionResult = new Transaction();
			/*String cardID = transactionData.getCard_id() + "";
			String amount = transactionData.getAmount() + "";
			String memberID = transactionData.getMember_id() + "";
			String posID = transactionData.getPos_id() + "";
			String postcode = transactionData.getPostcode() + "";
			String status = transactionData.getStatus() + "";
			String transaction_dt = transactionData.getTransaction_dt() + "";*/
			//System.out.println(cardID);
			// String memberID = transactionData;

			HTable hTable = new HTable(hBaseAdmin1.getConfiguration(), "amazon_hive");
			String rowkey = transactionData.getCard_id()+"_"+transactionData.getAmount()+"_"+transactionData.getTransaction_dt();
			Put p = new Put(Bytes.toBytes(rowkey));
			p.add(Bytes.toBytes("cf1"), Bytes.toBytes("amount"),Bytes.toBytes(transactionData.getCard_id() + ""));
			p.add(Bytes.toBytes("cf1"), Bytes.toBytes("card_id"),Bytes.toBytes(transactionData.getAmount() + ""));
			p.add(Bytes.toBytes("cf1"), Bytes.toBytes("member_id"),Bytes.toBytes(transactionData.getMember_id() + ""));
			p.add(Bytes.toBytes("cf1"), Bytes.toBytes("pos_id"),Bytes.toBytes(transactionData.getPos_id() + ""));
			p.add(Bytes.toBytes("cf1"), Bytes.toBytes("postcode"),Bytes.toBytes(transactionData.getPostcode()));
			p.add(Bytes.toBytes("cf1"), Bytes.toBytes("status"),Bytes.toBytes(transactionData.getStatus() + ""));
			p.add(Bytes.toBytes("cf1"), Bytes.toBytes("transaction_dt"),Bytes.toBytes(transactionData.getTransaction_dt() + ""));
			hTable.put(p); 
			//System.out.println("data inserted");
		} catch (

		Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null)
					table.close();
				hBaseAdmin1.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
