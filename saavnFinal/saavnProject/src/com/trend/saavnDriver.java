package com.trend;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 
public class saavnDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new saavnDriver(), args);
		System.exit(status);
	}

	public int run(String[] args) throws Exception {
		
		/*getjobName( ) : job name specified by the user
		getjobState( ) : Returns the  job current state
		isComplete ( ) : Checks whether the job is finished or not
		setInputFormatClass( )  :  Sets the input format for the job
		setjobName(String name) : Sets the job name specified by the user
		setOutputFormatClass( ) : Sets the output format for the job
		setMapperClass(Class) : Sets the mapper for the job
		setReducerClass(Class) : Sets the reducer for the job
		setPartitionerClass(Class) : Sets the partitioner for the job
		setCombinerClass(Class) : Sets the combiner for the job.
		Job job = new Job(getConf());*/
    	
	 Job job = new Job(getConf());
   	 job.setJobName("Trending Songs for dec");
   	
   	 job.setJarByClass(saavnDriver.class);

   	 job.setOutputKeyClass(Text.class);
   	 job.setOutputValueClass(Text.class);
   	 job.setMapOutputValueClass(RecordArrayWritable.class);
   	 job.setMapperClass(saavnMapper.class);
   	 job.setPartitionerClass(saavnPartioner.class);
   	 job.setCombinerClass(saavnCombiner.class);
   	 job.setReducerClass(saavnreducer.class);
 	 job.setNumReduceTasks(31);
   	 
   	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	   	try {
			return job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
	   	}
	   	return 0;
  }
}
