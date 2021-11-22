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

public class question4 {
	static final Logger logger = Logger.getLogger(App.class);
	static long window = 0L;
	public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException{
		
		String inputDirectory = args[0];
		String OutputDirectory = args[1];
		String currentUsersHomeDir = System.getProperty("user.home");
		String CheckpointDirectory = currentUsersHomeDir + File.separator + "checkpoint4";
		System.setProperty("hadoop.home.dir", args[0]+"\\winUtils");
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StockAnalysis4");
		
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
		JavaPairDStream<String, Double>  results= stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock.getVolume()))
		.updateStateByKey((values, currentState) -> {
			Double volume = 0.0;
			for (Double val: values) {
				volume = volume + val;
			}
			return Optional.of(volume);
		});
		
		 
		 results.print();
		 
		 results.foreachRDD(new VoidFunction<JavaPairRDD<String, Double>>() {
			 public void call(JavaPairRDD<String, Double> records) throws Exception {
			 // TODO Auto-generated method stub
				 records.saveAsTextFile(OutputDirectory+"/question4_output" + java.io.File.separator + System.currentTimeMillis());
			 }
			 });
		// results.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}



