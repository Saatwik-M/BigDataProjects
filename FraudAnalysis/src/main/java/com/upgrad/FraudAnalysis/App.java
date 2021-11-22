package com.upgrad.FraudAnalysis;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
	static final Logger logger = Logger.getLogger(App.class);
    public static void main( String[] args ) throws IOException
    {
    	Transaction transactionData = new Transaction();
    	 DistanceUtility distanceUtility = new DistanceUtility();
    	 String dateStart = "30-08-2016 18:28:26";
 		String dateStop = "31-10-2017 23:10:04";
    	 
    	String va = "348702330256514";
    	long id = Long.parseLong(va);
    	long amt = 3333333;
    	long memID = Long.parseLong("6599900000000001");
    	System.out.println(id);
    	transactionData.setCard_id(id);
    	transactionData.setAmount(amt);
		transactionData.setMember_id(memID);
		transactionData.setPos_id(4444);//248063406800722
		transactionData.setPostcode(10527);
		transactionData.setStatus("GENUINE");
		transactionData.setTransaction_dt("04-01-2018 23:53:41");
    	//long hh = Long.parseLong(va);
    // HBaseTransactionTableWriteDAO.putData(transactionData);
     
    // Transaction tran = HbaseReadDAO.getTransactionData(transactionData);
     //System.out.println(tran.getCard_id()+"~"+tran.getUcl()+"~"+tran.getPostcode()+"~"+tran.getMember_score());
     
     /* double distance = distanceUtility.getDistanceViaZipCode("81643", "81646");
     
     System.out.println(distance);
     
     SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
     Date d1 = null;
	 Date d2 = null;
     try {
		d1 = format.parse(dateStart);
		d2 = format.parse(dateStop);
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		

		//in milliseconds
		long diff = d2.getTime() - d1.getTime();

		long diffSeconds = (diff / 1000) ;
		long diffMinutes = diff / (24 * 60 * 60 * 1000);
		System.out.println(diffMinutes);*/
     
     
    }
}
