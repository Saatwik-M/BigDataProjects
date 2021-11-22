package com.trend;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class saavnCombiner extends
		Reducer<Text, RecordArrayWritable, Text, RecordArrayWritable> {

	public void reduce(Text key, Iterable<RecordArrayWritable> values,
			Context context) throws IOException {
		/**
		 *  Functionality of this combiner is to group record by key
		 *  eg [puUgIc0M , (01,1,17),(01,1,16)]
		 *  
		 */
		for (RecordArrayWritable val : values) {
			Writable[] recordArrayWritable = val.get();
			/**
			 *  Based on first parameter partition is selected for each records eg(01,1,17)
			 *  ie partition 0
			 */
			Record valu = (Record)recordArrayWritable[0];
			int recordArrayWritableLength = val.get().length;
			Record[] recordArray = new Record[recordArrayWritableLength];
			Writable[] dayWiseCountWritable = val.get();
			/**
			 *  Since only 1 record object will parse at a time
			 *  we have used [recordArrayWritableLength-1] 
			 */
			recordArray[recordArrayWritableLength-1] = (Record) dayWiseCountWritable[recordArrayWritableLength-1];
			try {
				context.write(key, new RecordArrayWritable(recordArray));
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
