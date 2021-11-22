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

public class question3 {
	static final Logger logger = Logger.getLogger(App.class);
	static long window = 10;
	static double previousAverageGain = 0.0;
	static double previousAverageLoss = 0.0;
	public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException{
		
		String inputDirectory = args[0];
		String OutputDirectory = args[1];
		String currentUsersHomeDir = System.getProperty("user.home");
		String CheckpointDirectory = currentUsersHomeDir + File.separator + "checkpoint3";
		//String CheckpointDirectory = "checkpoint3";
		System.setProperty("hadoop.home.dir", args[0]+"\\winUtils");
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StockAnalysis3");
		/**
		 * 
		 */
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		jssc.checkpoint(CheckpointDirectory);
		JavaDStream<String> input = jssc.textFileStream(inputDirectory);
		
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
		
		/** 
		  *  tuple(symbol,tuple(openPrice,ClosePrice))
		  */
		 JavaPairDStream<String, Tuple2<Double,Double>> stockWithOpenClosePrice= stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), new Tuple2<>(stock.getOpen(), stock.getClose())));
			
		 /**
		  *  Calculate profit or loss for a particular window
		  */
		 JavaPairDStream<String, Double> profit_or_loss_Tuples = stockWithOpenClosePrice.mapToPair(tuple -> new Tuple2<String, Double>(tuple._1,tuple._2._2-tuple._2._1));
		 
		 //profit_or_loss_Tuples.print();
		 
		 JavaPairDStream<String, Double> profit_Tuples = profit_or_loss_Tuples.filter(tuple -> tuple._2 >= 0);
		 JavaPairDStream<String, Double> loss_Tuples = profit_or_loss_Tuples.filter(tuple -> tuple._2 < 0);
		 JavaPairDStream<String, Double> unSignedLoss_Tuples = loss_Tuples.mapToPair(tuple -> new Tuple2<String, Double>(tuple._1,-1 * tuple._2));
		 
		 /**
		   * The very first calculations for average gain and average loss are simple 14-period averages.

			First Average Gain = Sum of Gains over the past 10 periods / 10.
			
			The second, and subsequent, calculations are based on the prior averages and the current gain loss:
			
			Average Gain = [(previous Average Gain) x 9 + current Gain] / 10.
			*/
		  JavaPairDStream<String, Double> avgGain =profit_Tuples
					  .updateStateByKey((values, currentState) -> {
							Double previousAverageGain = (double) currentState.or(0.0);
							Double AverageGain = 0.0 ;
							Double totalProfitPerStock = 0.0;
							for (Double val: values) {
								totalProfitPerStock = totalProfitPerStock + val;
								
							}
							/*
							 *  AverageGain over 10 mins window
							 */
							AverageGain = ((previousAverageGain * 9) + totalProfitPerStock) / window;
							return Optional.of(AverageGain);
						});
		 
		  avgGain.print();
		  
		  /**
		   * The very first calculations for average gain and average loss are simple 14-period averages.

 			First Average Loss = Sum of Losses over the past 9 periods / 10
			
			The second, and subsequent, calculations are based on the prior averages and the current gain loss:
			
			Average Loss = [(previous Average Loss) x 9 + current Loss] / 10.
		   */
		 JavaPairDStream<String, Double> avgLoss = unSignedLoss_Tuples
		  .updateStateByKey((values, currentState) -> {
				Double previousAverageGain = (double) currentState.or(0.0);
				Double AverageLoss = 0.0 ;
				Double totalLossPerStock = 0.0;
				for (Double val: values) {
					totalLossPerStock = totalLossPerStock + val;
					
				}
				/*
				 *  AverageLoss over 10 mins window
				 */
				AverageLoss = ((previousAverageGain * 9) + totalLossPerStock) / window;
				return Optional.of(AverageLoss);
			});
		 
		 
		  JavaPairDStream<String, Tuple2<Optional<Double>,Optional<Double>>>  avgGainLossPair = avgGain.fullOuterJoin(avgLoss);
		  
		  
		  /**
		   *  RSI is calculated
		   *  RS = Avg gain /Avg loss
				If Average Loss equals zero, a “divide by zero” situation occurs for RS and RSI is set to 100 by definition. 
				Similarly, RSI equals 0 when Average Gain equals zero.
		   */
		  JavaPairDStream<String, Double> RSI = avgGainLossPair.mapToPair(tuple -> 
		  { 
			  Double avgGainFinal = tuple._2._1.or(0.0);
			  Double avgLossFinal = tuple._2._2.or(0.0);
			  logger.info(avgGainFinal+"<== *******AverageGain==========AverageLoss*******  ===> "+ avgLossFinal);
			  Double relativeStrengthIndex = 0.0;
			  if(avgGainFinal == 0.0)
			  { 
				  relativeStrengthIndex = 0.0;
				 
			  }
			  else if(avgLossFinal == 0.0)
			  {
				  relativeStrengthIndex = 100.0;
			  }else
			  {
				  relativeStrengthIndex = 100 - (100/(1+(avgGainFinal/avgLossFinal)));
			  }
			  logger.info("<== **************relativeStrengthIndex  ===> "+ relativeStrengthIndex);
			  return new Tuple2<String, Double>(tuple._1,relativeStrengthIndex);
		  
		  });
		 
		  RSI.foreachRDD(new VoidFunction<JavaPairRDD<String, Double>>() {
				 public void call(JavaPairRDD<String, Double> records) throws Exception {
				 // TODO Auto-generated method stub
					 records.saveAsTextFile(OutputDirectory+"/question3_output" + java.io.File.separator + System.currentTimeMillis());
				 }
				 });
		  RSI.print();
		  
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}



