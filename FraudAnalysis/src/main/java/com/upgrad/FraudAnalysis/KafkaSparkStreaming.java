package com.upgrad.FraudAnalysis;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;



public class KafkaSparkStreaming {
	static final Logger logger = Logger.getLogger(App.class);
    public static void main(String[] args) throws Exception {
    	
    	String awsUrl = args[0];
    	String clientPort = args[1];
    	String masterPort = args[2];
    	String grpID = args[3];
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        DistanceUtility distanceUtility = new DistanceUtility();
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamingDemo").setMaster("local");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", grpID);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList("transactions-topic-verified");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaDStream<String> jds = stream.map(x -> x.value());

         JavaDStream<Transaction> transactionObjects = jds.flatMap(new FlatMapFunction<String, Transaction>() {
			
			private static final long serialVersionUID = 1L;
			ArrayList<Transaction> transactionArr= null;
			public Iterator<Transaction> call(String s)  {
				Object obj=JSONValue.parse(s);  
			    JSONObject jsonObject = (JSONObject) obj; 
			    System.out.println(s);
		            	Transaction transactionObj ;
					    transactionArr =  new ArrayList<Transaction>(); 
					    transactionObj = new Transaction();
							transactionObj.setCard_id((long)jsonObject.get("card_id"));
							transactionObj.setMember_id((long)jsonObject.get("member_id"));
							transactionObj.setAmount((long)jsonObject.get("amount"));
							transactionObj.setPos_id((long)jsonObject.get("pos_id"));
							transactionObj.setPostcode((long)jsonObject.get("postcode"));
							transactionObj.setTransaction_dt((String)jsonObject.get("transaction_dt"));
							transactionArr.add(transactionObj);
					 return transactionArr.iterator();   
		            
		        };
	});
        
      JavaDStream<Transaction> genuineTransaction = transactionObjects.filter(new Function<Transaction, Boolean>() {
        	@Override
			public Boolean call(Transaction obj) throws Exception {
				Transaction transactionResult = HbaseReadDAO.getTransactionData(obj,awsUrl,clientPort,masterPort);
				if(transactionResult != null)
				{
					boolean uclDecision = false;
					boolean memberScoreDecision = false;
					boolean postCodeDecision = false;
					boolean finalDecision = false;
					
					DecimalFormat df = new DecimalFormat("#");
				     df.setMaximumFractionDigits(8);
				     double uclDouble = Double.parseDouble(df.format(transactionResult.getUcl()));
				     //System.out.println(uclDouble);
				     
					if(transactionResult.getMember_score() >= 200)
						memberScoreDecision = true;
					
					if(uclDouble >= obj.getAmount())
						uclDecision = true;
					//System.out.println(transactionResult.getPostcode()+"=="+obj.getPostcode()+"");
					double distance = distanceUtility.getDistanceViaZipCode(transactionResult.getPostcode()+"", obj.getPostcode()+"");
					Date currentDate = null;
					 Date lastDate = null;
				     try {
				    	 currentDate = format.parse(obj.getTransaction_dt());
				    	 lastDate = format.parse(transactionResult.getTransaction_dt());
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
						
				     //in milliseconds
					long diff = currentDate.getTime() - lastDate.getTime();
					long diffSeconds = (diff / 1000) ;
					double distancethreshold = distance/diffSeconds ;
					
					if(distancethreshold <= 0.25 && diff >0)
						postCodeDecision = true;
				
					if(memberScoreDecision && uclDecision && postCodeDecision)
						finalDecision = true;
						
				return finalDecision;
				}
				return false;
			}
		});
        
        
        JavaDStream<String> genuineTransactionString = genuineTransaction.map(new Function<Transaction, String>() {

			@Override
			public String call(Transaction transaction) throws Exception {
				transaction.setStatus("GENUINE");
				HBaseTransactionTableWriteDAO.putData(transaction,awsUrl,clientPort,masterPort);
				HBaseLookUpTableWriteDAO.putData(transaction,awsUrl,clientPort,masterPort);
				//card_id,member_id,amount,postcode,pos_id,transaction_dt,status
				//String record = transaction.getCard_id()+","+transaction.getMember_id()+","+transaction.getAmount()+","+transaction.getPostcode()+","+transaction.getPos_id()+","+transaction.getTransaction_dt()+","+transaction.getStatus();
				return transaction.toString();
			}
		});
        
        genuineTransactionString.foreachRDD(new VoidFunction<JavaRDD<String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaRDD<String> rdd) {
            	//card_id,member_id,amount,postcode,pos_id,transaction_dt,status
                rdd.foreach(a -> System.out.println(a));
            }
        });
        
        genuineTransactionString.print();
        jssc.start();
        // / Add Await Termination to respond to Ctrl+C and gracefully close Spark
        // Streams
        jssc.awaitTermination();

    }
}