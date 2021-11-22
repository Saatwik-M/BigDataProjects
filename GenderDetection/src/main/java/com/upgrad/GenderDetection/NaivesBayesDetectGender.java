package com.upgrad.GenderDetection;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class NaivesBayesDetectGender {

	public static void main(String[] args) {
		final String path = "E:\\upgrad\\upgrad course\\Big Data Analytics\\genderDetection\\gender-classifier-DFE-791531.csv";
		 
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		System.setProperty("hadoop.home.dir", "C:\\Program Files\\winUtils\\bin");
		
		
		
		SparkSession sparkSession = SparkSession
				.builder().master("local[2]")
				.getOrCreate();
		
		// Read the file as a training dataset
				Dataset<Row> twitterData = sparkSession.read().
						format("csv").
						option("header","true").
						option("ignoreLeadingWhiteSpace",false). // you need this
						option("ignoreTrailingWhiteSpace",false).load(path).toDF();
				
				
				twitterData = twitterData.select(
						functions.col("_golden"),
						functions.col("gender:confidence"),
						functions.col("gender"),
						functions.col("description"),
						functions.col("text"),
						functions.col("profileimage"),
						functions.col("name"),
						functions.col("gender_gold"),
						functions.col("_trusted_judgments"));
				
				//twitterData.show(10);
				twitterData = twitterData.filter(functions.col("gender:confidence").isNotNull());
				twitterData = twitterData.filter(functions.col("_golden").isNotNull());
				/**
				 * cleaning text field 
				 */
				twitterData = twitterData.withColumn("text_normalize",functions.regexp_replace(functions.col("text"), "\\s\\W"," "));
				twitterData = twitterData.withColumn("text_normalized",functions.regexp_replace(functions.col("text_normalize"), "\\W\\s"," "))
						      .drop(functions.col("text_normalize"));
				twitterData = twitterData.withColumn("text_final_normalized",functions.regexp_replace(functions.col("text_normalized"), "\\s+"," "))
					      .drop(functions.col("text_normalized"));
				
				/**
				 * cleaning description field
				 */
				twitterData = twitterData.withColumn("description_normalize",functions.regexp_replace(functions.col("description"), "\\s\\W"," "));
				twitterData = twitterData.withColumn("description_normalized",functions.regexp_replace(functions.col("description_normalize"), "\\W\\s"," "))
						      .drop(functions.col("description_normalize"));
				twitterData = twitterData.withColumn("description_final_normalized",functions.regexp_replace(functions.col("description_normalized"), "\\s+"," "))
					      .drop(functions.col("description_normalize"));
				
				/**
				 *  taking records with confidence 1
				 */
				twitterData = twitterData.select("_golden","gender","gender:confidence","description_final_normalized","text_final_normalized","profileimage","name","gender_gold","_trusted_judgments").where(functions.col("gender:confidence").$eq$eq$eq(1));
				
				twitterData = twitterData.select("_golden","gender","gender:confidence","description_final_normalized","text_final_normalized","profileimage","name","gender_gold","_trusted_judgments").where(functions.col("description_final_normalized").isNotNull());
				
				twitterData = twitterData.select("_golden","gender","gender:confidence","description_final_normalized","text_final_normalized","profileimage","name","gender_gold","_trusted_judgments").where(functions.col("text_final_normalized").isNotNull());
				
				
				JavaRDD<Row> rdd1 = twitterData.toJavaRDD().repartition(4);
				System.out.println(rdd1.take(2).toString());
				//Function to map.
				JavaRDD<Row> rdd2 = rdd1.map( new Function<Row, Row>() {

					public Row call(Row iRow) throws Exception {
						
						
	double gender = (iRow.getString(1).equals("male") ? 0.0 : (iRow.getString(1).equals("female")? 1.0 : (iRow.getString(1).equals("brand")? 2.0 : 3.0)));
	String data = iRow.getString(3) + " "+ iRow.getString(4);
	Row retRow = RowFactory.create( iRow.getString(1), data);
						
						return retRow;
					}

				});
				
				//Create the schema for the data to be loaded into Dataset.
				StructType smsSchema = DataTypes
						.createStructType(new StructField[] {
								DataTypes.createStructField("gender", DataTypes.StringType, true),
								DataTypes.createStructField("message", DataTypes.StringType, true)
							});
				
				Dataset<Row> smsCleansedDf = sparkSession.createDataFrame(rdd2, smsSchema);
				
				smsCleansedDf.show();
				//Create Data Frame back.
				//smsCleansedDf.show();
				/*--------------------------------------------------------------------------
				Prepare for Machine Learning - setup pipeline
				--------------------------------------------------------------------------*/
				// Split the data into training and test sets (30% held out for testing).
				Dataset<Row>[] splits = smsCleansedDf.randomSplit(new double[]{0.7, 0.3});
				Dataset<Row> trainingData = splits[0];
				Dataset<Row> testData = splits[1];
				
				/*--------------------------------------------------------------------------
				Perform machine learning. - Use the pipeline
				--------------------------------------------------------------------------*/
				
				StringIndexerModel labelindexer = new StringIndexer()
						.setInputCol("gender")
						.setOutputCol("label").fit(trainingData);
				
				Tokenizer tokenizer = new Tokenizer()
											.setInputCol("message")
											.setOutputCol("words");
				
				
				HashingTF hashingTF = new HashingTF()
										  .setInputCol("words")
										  .setOutputCol("rawFeatures");
				IDF idf = new IDF()
							.setInputCol("rawFeatures")
							.setOutputCol("features");
				
				NaiveBayes nbClassifier = new NaiveBayes()
												.setLabelCol("label")
												.setFeaturesCol("features");
				
				Pipeline pipeline = new Pipeline()
						  .setStages(new PipelineStage[]
								  {labelindexer,tokenizer, hashingTF, idf, nbClassifier});
				
				PipelineModel plModel = pipeline.fit(trainingData);
				
				Dataset<Row> predictions = plModel.transform(testData);
				predictions.show(5);
				
				
				//View results
				System.out.println("Result sample :");
				predictions.show(5);
				
				//View confusion matrix
				System.out.println("Confusion Matrix :");
				predictions.groupBy(col("label"), col("prediction")).count().show();
				
				//Accuracy computation
				MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
						  .setLabelCol("label")
						  .setPredictionCol("prediction")
						  .setMetricName("accuracy");
						double accuracy = evaluator.evaluate(predictions);
						System.out.println("Accuracy = " + Math.round( accuracy * 100) + " %" );
						
				MulticlassClassificationEvaluator f1evaluator = new MulticlassClassificationEvaluator()
								  .setLabelCol("label")
								  .setPredictionCol("prediction")
								  .setMetricName("f1");
								double fi = f1evaluator.evaluate(predictions);
								System.out.println("f1 score = " + fi );
										
				MulticlassClassificationEvaluator recallevaluator = new MulticlassClassificationEvaluator()
								  .setLabelCol("label")
								  .setPredictionCol("prediction")
								  .setMetricName("weightedRecall");
								double recall = recallevaluator.evaluate(predictions);
								System.out.println("recall score = " + recall );
												
				MulticlassClassificationEvaluator precisionevaluator = new MulticlassClassificationEvaluator()
								  .setLabelCol("label")
								  .setPredictionCol("prediction")
								  .setMetricName("weightedPrecision");
								double precision = precisionevaluator.evaluate(predictions);
								System.out.println("precision score = " + precision );
				

	}

}
