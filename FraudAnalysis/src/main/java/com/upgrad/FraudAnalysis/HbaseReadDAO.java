package com.upgrad.FraudAnalysis;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/** * HBase DAO class that provides different operational handlers. */
public class HbaseReadDAO {
	/**
	 * * * @param transactionData * @return get member's score from look up HBase
	 * table. * @throws IOException
	 */
	public static Transaction getTransactionData(Transaction transactionData,String awsUrl,String clientPort,String masterPort) throws IOException {
	@SuppressWarnings("deprecation")
	Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin(awsUrl,clientPort,masterPort);
		HTable table = null;
		Transaction transactionResult = null;
		try {
			transactionResult = new Transaction();
			String cardID = transactionData.getCard_id()+"";
			
			Get g = new Get(Bytes.toBytes(cardID));
			transactionResult.setCard_id(Long.parseLong(cardID));
			table = new HTable(hBaseAdmin1.getConfiguration(), "look_up_hive");
			Result result = table.get(g);
			if(result.size() == 0)
			transactionResult = null;
			byte[] memberScoreValue = result.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("score"));
			byte[] UCLvalue = result.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("UCL"));
			byte[] postcodeValue = result.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("postcode"));
			byte[] transactionDate = result.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("transaction_dt"));
			
			if (memberScoreValue != null) {
				transactionResult.setMember_score(Integer.parseInt(Bytes.toString(memberScoreValue)));
			}
			if (UCLvalue != null) {
				transactionResult.setUcl(Double.parseDouble(Bytes.toString(UCLvalue)));
			}
			if (postcodeValue != null) {
				transactionResult.setPostcode(Long.parseLong(Bytes.toString(postcodeValue)));
			}
			if (transactionDate != null) {
				transactionResult.setTransaction_dt(Bytes.toString(transactionDate));
			}
		} catch (Exception e) {
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
		return transactionResult;
	}
}