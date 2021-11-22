package com.upgrad.Spark_Stock_Analysis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import scala.Tuple2;

public class question2 {
	static final Logger logger = Logger.getLogger(App.class);
	static long window = 0L;
	public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException{
		
		String inputDirectory = args[0];
		String OutputDirectory = args[1];
		String currentUsersHomeDir = System.getProperty("user.home");
		String CheckpointDirectory = currentUsersHomeDir +  "//checkpoint2";
		//String CheckpointDirectory = "checkpoint2";
		System.setProperty("hadoop.home.dir", args[0]+"\\winUtils");
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StockAnalysis2");
		/**
		 * 
		 */
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		jssc.checkpoint(CheckpointDirectory);
		JavaDStream<String> input = jssc.textFileStream(inputDirectory);
		//input.print();
		
		/**
		 *  As sliding window is 5 mins and window is 10 mins
		 *  After 1st sliding window : It will consider only five mins stock ie 1-5 min stock
		 *  After 2st sliding window : It will consider only 10 mins stock ie 1-10 min stock
		 *  After 3st sliding window : It will consider only 10 mins stock ie 6-15 min stock
		 */
		
		
		JavaDStream<Stock> stocks = input.window(Durations.seconds(600), Durations.seconds(300))
									.flatMap(new FlatMapFunction<String, Stock>() {
											public Iterator<Stock> call(String s)  {
													JSONArray json = (JSONArray) JSONSerializer.toJSON(s);        
												    Stock stockObj ;
												    JSONObject jsonObj;
												    ArrayList<Stock> stockArr= new ArrayList<Stock>(); 
												    logger.info("**************  ===> "+ json);
												    for(int i=0 ; i< json.size() ;i++)
													{
														stockObj = new Stock();
														jsonObj = json.getJSONObject(i).getJSONObject("priceData");
														stockObj.setSymbol((String)json.getJSONObject(i).get("symbol"));
														stockObj.setTimestamp((String)json.getJSONObject(i).get("symbol"));
														stockObj.setOpen(Double.parseDouble((String)jsonObj.get("open")));
														stockObj.setHigh(Double.parseDouble((String)jsonObj.get("high")));
														stockObj.setLow(Double.parseDouble((String)jsonObj.get("low")));
														stockObj.setClose(Double.parseDouble((String)jsonObj.get("close")));
														stockObj.setVolume(Double.parseDouble((String)jsonObj.get("volume")));
														stockArr.add(stockObj);
													}
												return stockArr.iterator();
												} 
										});
		
		JavaPairDStream<String, Tuple2<Double,Double>> stockWithOpenClosePrice= stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), new Tuple2<>(stock.getOpen(), stock.getClose())));
		 
		 /**
			 * window is calculate based on sliding window.
			 * eg if 4 records are fetched in 1 sliding window of 5 min
			 * 			window  = 4 records * 4 stocks / 4  = 4 ;
			 * 				where 4 records * 4 stocks = rdd.count()
			 * 	if 6 records are fetched in current sliding window 
			 *  	window = (4 records of previous window + 6 records of current window )* 4 stocks  /4 = 10
			 *  	where (4 records of previous window + 6 records of current window )* 4 stocks = rdd.count()
			 */
			stocks.foreachRDD(rdd -> {
				window = rdd.count()/4;
				/**
				 *  if 
				 *  	less than 5 records are found in 1st sliding window OR records are found in 1st sliding window is
				 *  	greater than  5 but less than 10 ie  10 > records > 5  
				 *  then 
				 *  	also window will remain 5 min window
				 *  
				 * else if 
				 * 		records are found in current sliding window is more than 10 ie records > window
				 * then
				 * 		window = 10
				 *   
				 */
				if((window > 5 && window < 10) || (window < 5)) 
					window = 5;
				else if(window > 10)
					window = 10;
				System.out.printf("record count: %d\n", rdd.count());
				System.out.printf("window: %d\n", window);
				});
			
		 /**
		  *  join both stockWithClosePrice & stockWithOpenPrice and aggregate based on key
		  *  eg 
		  */	
			
		 JavaPairDStream<String, Double> results = stockWithOpenClosePrice
		 .reduceByKey((accum, tuple) -> new Tuple2<>((accum._1+tuple._1) , (accum._2+tuple._2)))
		 .map(tuple -> 
		 {
			return new Tuple2<>(tuple._1, new Tuple2<>((tuple._2._1 /window) , (tuple._2._2 /window))); //tuple(average opening price,average closing price)
		 
		 }) // calculate avg
		 .mapToPair(tuple -> new Tuple2<>(tuple._1, (tuple._2._2 - tuple._2._1 )));//profit or loss (average closing price - average opening price)
		
		 results.foreachRDD(new VoidFunction<JavaPairRDD<String, Double>>() {
			 public void call(JavaPairRDD<String, Double> records) throws Exception {
			 // TODO Auto-generated method stub
				 records.saveAsTextFile(OutputDirectory+"/question2_output" + java.io.File.separator + System.currentTimeMillis());
			 }
			 });
		 results.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}



