package spark.question1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class lookUpRDD {
	public static void main(String[] args) throws Exception {

		/********
		 * Problem Statement *********
		 * 
		 * Fetch the record having VendorID as '2' AND tpep_pickup_datetime as
		 * '2017-10-01 00:15:30' AND tpep_dropoff_datetime as '2017-10-01 00:25:11' AND
		 * passenger_count as '1' AND trip_distance as '2.17'
		 */
		JavaSparkContext sc = null;
		try {
			// Logger.getLogger("org").setLevel(Level.ERROR);
			/**
			 * "System.setProperty" is written to avoid "java.io.IOException: Could not
			 * locate executable null\bin\winutils.exe in the Hadoop binaries."
			 */
			System.setProperty("hadoop.home.dir", "/user/root/spark_assignment/winUtils/bin/winutils.exe");
			// C:\\Program Files\\winUtils
			SparkConf conf = new SparkConf().setAppName("lookUp").setMaster("local[2]");

			sc = new JavaSparkContext(conf);

			JavaRDD<String> tripDetails = sc.textFile(args[0]);

			/**
			 * To remove Header
			 */
			JavaRDD<String> cleanTripDetails = tripDetails.filter(line -> (isNotHeader(line) && !(line.isEmpty())));

			/**
			 * Filter for VendorID as '2'
			 */
			JavaRDD<String> tripDetailsWithVendor = cleanTripDetails
					.filter(line -> Integer.valueOf(line.toString().split(",")[0]) == 2);

			/**
			 * Filter for tpep_pickup_datetime as '2017-10-01 00:15:30'
			 */
			JavaRDD<String> details1 = tripDetailsWithVendor
					.filter(line -> line.split(",")[1].equals("2017-10-01 00:15:30"));

			/**
			 * Filter for tpep_dropoff_datetime as '2017-10-01 00:25:11'
			 */
			JavaRDD<String> details2 = details1.filter(line -> line.split(",")[2].equals("2017-10-01 00:25:11"));

			/**
			 * Filter for passenger_count as '1'
			 */
			JavaRDD<String> details3 = details2.filter(line -> Integer.valueOf(line.toString().split(",")[3]) == 1);

			/**
			 * Filter for trip_distance as '2.17'
			 */

			JavaRDD<String> details4 = details3.filter(line -> Double.valueOf(line.toString().split(",")[4]) == 2.17);

			/**
			 * Save result in question1 folder
			 */
			details4.saveAsTextFile(args[1]);
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
