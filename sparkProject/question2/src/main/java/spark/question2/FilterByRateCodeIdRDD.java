package spark.question2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterByRateCodeIdRDD {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = null;
		try {
			// Logger.getLogger("org").setLevel(Level.ERROR);
			System.setProperty("hadoop.home.dir", "/user/root/spark_assignment/winUtils/bin/winutils.exe");
			SparkConf conf = new SparkConf().setAppName("lookUp").setMaster("local[*]");

			/********
			 * Problem Statement *********
			 * 
			 * Filter all the records having RatecodeID as 4.
			 */

			sc = new JavaSparkContext(conf);

			JavaRDD<String> tripDetails = sc.textFile(args[0]);

			/**
			 * To remove Header
			 */
			JavaRDD<String> cleanTripDetails = tripDetails.filter(line -> (isNotHeader(line) && !(line.isEmpty())));

			/**
			 * Filter for RatecodeID as 4
			 */
			JavaRDD<String> tripDetailsWithRateCode = cleanTripDetails
					.filter(line -> Integer.valueOf(line.toString().split(",")[5]) == 4);

			/**
			 * Save result in question2 folder
			 */
			tripDetailsWithRateCode.saveAsTextFile(args[1]);
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
