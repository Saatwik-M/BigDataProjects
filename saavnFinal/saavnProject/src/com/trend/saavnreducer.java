package com.trend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/** Each reducer with contain Iterable<RecordArrayWritable> values as below
 * eg [puUgIc0M , (01,1,17),(01,1,16)]
 */
public class saavnreducer extends Reducer<Text, RecordArrayWritable, Text, Text> {
	
	Map<String, Integer> map = new ConcurrentHashMap<String, Integer>();
	int combinerCount = 0;
	
	/**
	 * Calculate sum of weights for a song on a particular day
	 * ie for example calculate sum of weight for all song 'puUgIc0M' for day 1 which will be reduced to partioner 0
	 */
	public void reduce(Text key, Iterable<RecordArrayWritable> values, Context context)
			throws IOException {
		combinerCount++;
		int sum = 0;
		String song = key.toString();
		for (RecordArrayWritable val: values) {
			Writable[] records = val.getRecordArrayWritable();
			Record re = (Record)records[0];
			sum = sum + re.getWeight();
			}
		map.put(song, sum);
	}
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
try {
		/**
		 *  Utils.sortByValues(map) take map as input and sorts it by value.
		 *  map structure => [songId(Key),TotalNoOfCount(Value)]
		 */
			Map<String, Integer> sortedMap = Utils.sortByValues(map);
			Iterator itr = sortedMap.entrySet().iterator();
			int rank = 1;
			
        	/**
        	 * to get top 100 records
        	 */
        	while (itr.hasNext() && rank <= 100 && combinerCount == map.size()) {
	            Map.Entry pair = (Map.Entry)itr.next();
	            context.write(new Text(pair.getKey().toString()), new Text(String.valueOf(rank)));
	            rank++;
	        }
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }

}
