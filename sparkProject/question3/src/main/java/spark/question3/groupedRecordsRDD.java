package spark.question3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class groupedRecordsRDD {

	public static void main(String[] args) {

		/**********
		 * Problem Statement *********
		 * 
		 * Group By all the records based on payment type and find the count for each
		 * group. Sort the payment types in ascending order of their count.
		 */

		JavaSparkContext sc = null;
		try {
			// Logger.getLogger("org").setLevel(Level.ERROR);
			System.setProperty("hadoop.home.dir", "/user/root/spark_assignment/winUtils/bin/winutils.exe");
			SparkConf conf = new SparkConf().setAppName("groupBy").setMaster("local[2]");

			sc = new JavaSparkContext(conf);

			/**
			 * function used submation of records (Called in reduceByKey)
			 */
			Function2<Integer, Integer, Integer> reduceSumFunc = (accum, n) -> (accum + n);
			JavaRDD<String> tripDetails = sc.textFile(args[0]);

			/**
		     * To remove Header
		     */
		    JavaRDD<String> cleanTripDetailsNoHeaders = tripDetails.filter(line -> (isNotHeader(line) && !(line.isEmpty())));
		    
			/**
			 * Generate map syntax (payment type value -> 1)
			 */
			JavaPairRDD<Integer, Integer> rddX = cleanTripDetailsNoHeaders
					.mapToPair(line -> new Tuple2<Integer, Integer>(Integer.valueOf(line.toString().split(",")[9]), 1));

			/**
			 * To perform order By on grouped By records
			 */
			JavaPairRDD<Integer, Integer> rddY = rddX.reduceByKey(reduceSumFunc);

			JavaPairRDD<Integer, Integer> rddYSwapMap = rddY
					.mapToPair(line -> new Tuple2<Integer, Integer>(line._2, line._1)).sortByKey(true);

			JavaPairRDD<Integer, Integer> rddYFinal = rddYSwapMap
					.mapToPair(line -> new Tuple2<Integer, Integer>(line._2, line._1));

			// Print tuples
			for (Tuple2<Integer, Integer> element : rddYFinal.collect()) {
				System.out.println("(" + element._1 + ", " + element._2 + ")");
			}
			/**
			 * Save result in question3 folder
			 */
			rddYFinal.saveAsTextFile(args[1]);
			// System.out.println(rddY.collect());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				sc.stop();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}

	/**
	 * Function to trace Header
	 */
	private static boolean isNotHeader(String line) {
		return !(line.startsWith("VendorID") && line.contains("total_amount"));
	}

}
