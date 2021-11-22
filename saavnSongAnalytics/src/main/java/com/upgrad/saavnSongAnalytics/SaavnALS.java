package com.upgrad.saavnSongAnalytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class SaavnALS {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		//System.setProperty("hadoop.home.dir", "C:\\Program Files\\winUtils\\bin");
		System.setProperty("hadoop.home.dir", args[0]);
		// Create a SparkSession
		
		SparkSession sparkSession = SparkSession.builder().config("spark.hadoop.fs.s3a.access.key", args[1])

				.config("spark.hadoop.fs.s3a.secret.key", args[2]).appName("sampleApp").master("local[*]")

				.getOrCreate();
		
		/*SparkSession sparkSession = SparkSession
				.builder().master("local[2]")
				.getOrCreate();*/
		
		/*String clickStreamDataPath = "s3a://saavn23042018/Data100MB/sample100mb.csv";
		String metaDataPath = "s3a://saavn23042018/metadata/*";
		String notificationArtistPath = "s3a://saavn23042018/notification_actor/notification.csv";
		String notificationClickPath = "s3a://saavn23042018/notification_click/*";*/
		
		/*String clickStreamDataPath = "E:\\upgrad\\upgrad course\\Big Data Analytics\\saavn\\sample100mb.csv";
		String metaDataPath = "E:\\upgrad\\upgrad course\\Big Data Analytics\\saavn\\metadata\\*";
		String notificationArtistPath = "E:\\upgrad\\upgrad course\\Big Data Analytics\\saavn\\notification.csv";
		String notificationClickPath = "E:\\upgrad\\upgrad course\\Big Data Analytics\\saavn\\noti_click\\*";
		*/
		String clickStreamDataPath = args[3];
		String metaDataPath = args[4];
		String notificationArtistPath = args[5];
		String notificationClickPath = args[6];
		String output_path = args[7];
		
		// Read the file as a training dataset
		Dataset<Row> clickStreamDataCSVClean = sparkSession.read().
				format("csv").
				option("header","false").
				option("ignoreLeadingWhiteSpace",false). // you need this
				option("ignoreTrailingWhiteSpace",false).load(clickStreamDataPath).toDF("userID","timestamp","songID","date");
		
		/*Dataset<Row> metaDataCSV = sparkSession.read().
				format("csv").
				option("header","false").
				option("ignoreLeadingWhiteSpace",false). // you need this
				option("ignoreTrailingWhiteSpace",false).load(metaDataPath).toDF("songID","ArtistID1");*/
		
		
		Dataset<Row> metaDataCSVClean = sparkSession.read().option("header", "false").csv(metaDataPath)
				.select(functions.col("_c0").as("songID"), functions.col("_c1").as("ArtistID1"));
		
		
		// Ignore rows having null values
		clickStreamDataCSVClean = clickStreamDataCSVClean.na().drop();
		metaDataCSVClean = metaDataCSVClean.na().drop();
		
		StringIndexer userIndexer = new StringIndexer()
				  .setInputCol("UserID")
				  .setOutputCol("userIDIndex");
		
		StringIndexer songIndexer = new StringIndexer()
				  .setInputCol("SongID")
				  .setOutputCol("songIDIndex");
		
		
		//Dataset<Row> grouped = clickStreamDataCSVClean.groupBy("songID").agg(functions.count("songID").alias("count"));
		
		Dataset<Row> grouped = clickStreamDataCSVClean.groupBy("UserID", "SongID").count();
		grouped = userIndexer.fit(grouped).transform(grouped);
		grouped= songIndexer.fit(grouped).transform(grouped);
		
		/**
		 * columns generated from above transform [UserID|SongID|count|userIDIndex|songIDIndex]
		 */
		
		ALS als = new ALS()
				  .setUserCol("userIDIndex")
				  .setItemCol("songIDIndex")
				  .setRatingCol("count")
				  .setImplicitPrefs(true)
				  .setMaxIter(5)
				  . setRegParam(0.01);
		
		org.apache.spark.ml.recommendation.ALSModel model = als.fit(grouped);
		
		Dataset <Row> userFeatures = model.userFactors();
		userFeatures = userFeatures.withColumn("rank1", userFeatures.col("features").getItem(0))
		  .withColumn("rank2", userFeatures.col("features").getItem(1))
		.withColumn("rank3", userFeatures.col("features").getItem(2))
		.withColumn("rank4", userFeatures.col("features").getItem(3))
		.withColumn("rank5", userFeatures.col("features").getItem(4))
		.withColumn("rank6", userFeatures.col("features").getItem(5))
		.withColumn("rank7", userFeatures.col("features").getItem(6))
		.withColumn("rank8", userFeatures.col("features").getItem(7))
		.withColumn("rank9", userFeatures.col("features").getItem(8))
		.withColumn("rank10", userFeatures.col("features").getItem(9));
		
		//userFactors.show();
		userFeatures = userFeatures.drop("features");
		ArrayList<String> inputColsList = new ArrayList<String>(Arrays.asList(userFeatures.columns()));

		//Make single features column for feature vectors 
		String[] inputCols = inputColsList.parallelStream().toArray(String[]::new);
		
		//Prepare dataset for training with all features in "features" column
		VectorAssembler assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("features");
		Dataset<Row> finalData = assembler.transform(userFeatures);
		//finalData.show();
		 
		// to determine k val
		/*
		List<Integer> numClusters = Arrays.asList(180,200,220,250,280);
		for (Integer k : numClusters) {
			KMeans kmeans = new KMeans().setK(k).setSeed(1L);
			KMeansModel kmodel = kmeans.fit(finalData);
		
			//Within Set Sum of Square Error (WESSE).					
			double WSSSE = kmodel.computeCost(finalData);
			System.out.println("WSSSE = " + WSSSE);
			
			//Shows the results
			Vector[] centers = kmodel.clusterCenters();
			System.out.println("Cluster Centers: ");
			for (Vector center: centers) {
			  System.out.println(center);
			}
								
		}*/
		
		// Trains a k-means model
				KMeans kmeans = new KMeans().setK(300);
				KMeansModel kMeanModel = kmeans.fit(finalData);
				
				// Make predictions
				Dataset<Row> predictions = kMeanModel.transform(finalData);
				/**
				 * columns generated from above transform [id|features| prediction]
				 */
				//predictions.show(200);
				
				/**
				   columns generated from below join [UserID|  SongID|userIDIndex|  songID|ArtistID1]
				 */
				Dataset <Row> artistClusterMapping = grouped
						.drop(grouped.col("songIDIndex"))
						.drop(grouped.col("count"))
						.drop(grouped.col("userID"))
						.join(metaDataCSVClean, grouped.col("songID").equalTo(metaDataCSVClean.col("songID")), "inner")
						.drop(grouped.col("songID"));
				
				/**
				   columns generated from below join [ArtistID1|prediction]
				 */
				artistClusterMapping = artistClusterMapping.join(predictions, artistClusterMapping.col("userIDIndex").equalTo(predictions.col("id")), "inner")
						.select(artistClusterMapping.col("ArtistID1"),
								predictions.col("prediction")
								);
				
				/**
				   AGGartistClusterMapping is used count artist count in very cluster
				    columns generated from below join [prediction|ArtistID1|artistCount]
				 */
				Dataset <Row> AGGartistClusterMapping = artistClusterMapping.groupBy("prediction","ArtistID1").agg(functions.count("*").alias("artistCount"));
				/**
				   To Associate 1 artist to cluster based on max count
				   columns generated from below join [prediction|ArtistID1|artistCount| rn]
				 */
				WindowSpec windowSpec = Window.partitionBy(AGGartistClusterMapping.col("prediction")).orderBy(AGGartistClusterMapping.col("artistCount").desc());
				AGGartistClusterMapping = AGGartistClusterMapping.withColumn("rn", functions.row_number().over(windowSpec));
				/**
				 * select artist with max count in a cluster
				 * columns generated from below join [prediction|ArtistID1|artistCount]
				 */
				Dataset <Row> topArtist = AGGartistClusterMapping.where(AGGartistClusterMapping.col("rn").$eq$eq$eq(1)).drop(AGGartistClusterMapping.col("rn"));
				
				System.out.println("######################Top Artist for each cluster###########################");
				//topArtist.show();
				
				Dataset<Row> notification_Artist_CSV = sparkSession.read().
						format("csv").
						option("header","false").
						option("ignoreLeadingWhiteSpace",false). // you need this
						option("ignoreTrailingWhiteSpace",false).load(notificationArtistPath).toDF("notificationID","ArtistID");
				
				/**
				 *  cluster Notification mapping
					columns generated from below join [notificationID|ArtistID|prediction]
				 */
				
				Dataset <Row> User_Notifcation_mapping = notification_Artist_CSV.join(topArtist, notification_Artist_CSV.col("ArtistID").equalTo(topArtist.col("ArtistID1")), "inner")
						.drop(topArtist.col("ArtistID1"))
						.drop(topArtist.col("artistCount"));
				
				/**
				 *  Assign notification to user based on cluster Id and popular Artist for that cluster.
					columns generated from below join [notificationID|prediction|ArtistID| id]
				 * */
				User_Notifcation_mapping = User_Notifcation_mapping.join(predictions, predictions.col("prediction").equalTo(User_Notifcation_mapping.col("prediction")), "inner")
						.select(User_Notifcation_mapping.col("notificationID"),
								User_Notifcation_mapping.col("prediction"),
								User_Notifcation_mapping.col("ArtistID"),
								predictions.col("id"));
				
				/**
				 *  Final predicting notification to user
				 columns generated from below join [notificationID|prediction|ArtistID|UserID|  SongID]
				 */
				
				Dataset <Row> final_user_Notification = User_Notifcation_mapping.join(grouped, grouped.col("userIDIndex").equalTo(User_Notifcation_mapping.col("id")), "inner")
				.drop(User_Notifcation_mapping.col("id"))
				.drop(grouped.col("userIDIndex"))
				.drop(grouped.col("songIDIndex"))
				.drop(grouped.col("songID"))
				.drop(grouped.col("count"));
				System.out.println("final_user_Notification");
				final_user_Notification.repartition(final_user_Notification.col("prediction")).write().partitionBy("notificationID").mode(SaveMode.Overwrite).csv(output_path+"/notiPrediction");
				
				/**
				 * a
				 * For CTR Calculation
				 * 
				 */
				
				Dataset <Row> TotalUserPerCluster = final_user_Notification.groupBy("prediction").agg(functions.countDistinct("UserID").alias("TotalUserPerCluster"));
				
				Dataset<Row> notification_Click_CSV = sparkSession.read().
						format("csv").
						option("header","false").
						option("ignoreLeadingWhiteSpace",false). // you need this
						option("ignoreTrailingWhiteSpace",false).load(notificationClickPath).toDF("notificationID","userID","date");
				
				/**
				 *  user clicking notification count
				 *  columns generated from below join [ArtistID|notificationID|NotificationClickedByUser]
				 */
				
				Dataset <Row> CTR = final_user_Notification.join(notification_Click_CSV, final_user_Notification.col("notificationID").equalTo(notification_Click_CSV.col("notificationID")).and(final_user_Notification.col("userID").equalTo(notification_Click_CSV.col("userID"))), "inner")
						.select(final_user_Notification.col("userID"),
								final_user_Notification.col("ArtistID")).groupBy("ArtistID").agg(functions.countDistinct("userID").alias("NotificationClickedByUser"));
				/**
				 * combine the three clusters and report a common CTR IF ArtistID is same
				 * columns generated from below join [ArtistID|NotificationClickedByUser]
				 */
				CTR = CTR.groupBy(CTR.col("ArtistID")).agg(functions.sum(CTR.col("NotificationClickedByUser")).alias("NotificationClickedByUser"));
				
				/**
				 * columns generated from below join [ArtistID|NotificationClickedByUser|prediction]
				 */
				CTR  = CTR.join(topArtist, CTR.col("ArtistID").equalTo(topArtist.col("ArtistID1")), "inner")
						.drop(topArtist.col("ArtistID1"))
						.drop(topArtist.col("artistCount"));
				
				/**
				 * columns generated from below join [ArtistID|NotificationClickedByUser|prediction|TotalUserPerCluster]
				 */
				CTR  = CTR.join(TotalUserPerCluster, CTR.col("prediction").equalTo(TotalUserPerCluster.col("prediction")), "inner")
						.drop(CTR.col("prediction"));
				
				/**
				 * columns generated from below join [ArtistID|NotificationClickedByUser|prediction|TotalUserPerCluster|CTR]
				 */
				CTR  = CTR.withColumn("CTR", CTR.col("NotificationClickedByUser").divide(CTR.col("TotalUserPerCluster")).multiply(100));
				CTR.repartition(CTR.col("prediction")).write().partitionBy("prediction").mode(SaveMode.Overwrite).csv(output_path+"/CTR");
				sparkSession.stop();
				
				
	}	
	
}
