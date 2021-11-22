package com.trend;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Logger;

class saavnMapper extends Mapper<LongWritable, Text, Text, RecordArrayWritable> {

	Map<String, Integer> map = new ConcurrentHashMap<String, Integer>();
	BloomFilter bloomFilter = new BloomFilter(18000000, 3, Hash.MURMUR_HASH);
	private Logger log = Logger.getLogger(saavnMapper.class);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		List<String> daysList = Arrays.asList(new String[] { "01", "02", "03",
				"04", "05", "06", "07", "08", "09", "10", "11", "12", "13",
				"14", "15", "16", "17", "18", "19", "20", "21", "22", "23",
				"24", "25", "26", "27", "28", "29", "30", "31" });
		
		if (!(value.toString().isEmpty() || 
			value.toString().equalsIgnoreCase(""))) {

			String values[] = value.toString().split("\\,");
			/**
			 * obtaining song Id, day of month ,hour of the day.
			 */

			String song = values[0];
			String date = values[values.length - 1].trim();
			int hour = Integer.parseInt(values[values.length - 2].trim());
			String day = date.split("\\-")[2];
			
			Record[] records = new Record[1];
			/**
			 * Classification of time window
			 * 
			 * window1 = 00 to 05
			 * window2 = 06 to 11
			 * window3 = 12 to 17
			 * window4 = 18 to 23
			 * 
			 * Based on hour at which song is played we calculate current and previous window.
			 */
			String window = "";
			String previousWindow = "";
			if (0 <= hour && hour <= 05) {
				window = song + "" + day + "window1";
				
				int previousDayIndex = daysList.indexOf(day)-1;
				if (Integer.parseInt(day) == 01) {
					/**
					 * 	If song(puUgIc0M) is played in 00-05 hr of 1st december
					 * current window for that song is : puUgIc0M01window1(song+day+timeWindow in which song is played)
					 * previous window for that song is set to  'previousWindowNotAvailablev' because previous day of 1st
					 * december is not available with us
					 */
					previousWindow = "previousWindowNotAvailable";
				} else {
					/**
					 * 	If song(puUgIc0M) is played in 00-05 hr other than 1st December
					 * eg 2nd december
					 * current window for that song is : puUgIc0M02window1(song+day+timeWindow in which song is played)
					 * previous window for that song is : puUgIc0M01window4(previous window for day 2 window1 will be day 1 window4)
					 */
					previousWindow = song + ""+ daysList.get(previousDayIndex) + "window4";
				}

			} else if (06 <= hour && hour <= 11) {
				window = song + "" + day + "window2";
				previousWindow = song + "" + day + "window1";
			} else if (12 <= hour && hour <= 17) {
				window = song + "" + day + "window3";
				previousWindow = song + "" + day + "window2";
			} else if (18 <= hour && hour <= 23) {
				window = song + "" + day + "window4";
				previousWindow = song + "" + day + "window3";
			}
			
			/**
			 * To achieves trendness of song we are manipulating weight for songs.
			 * 1) If a song(puUgIc0M) is present in previous window , song weight = previous window weight for a song(puUgIc0M) + 1 .
			 * 2) If a song is not present in previous window, song weight = 1. 
			 * 
			 * This way we can calculate trending ie if song is present in previous window we increment weight for that song by 1.
			 * If song is not present, song might not be trending or may be sudden spike but it may also be trending song 
			 * but to analyse that we need to consider it next window.
			 * 
			 * Note:We are checking if song is present in previous window or not by using bloom Filters
			 * 	 
			 */
			
			int weight = 0;
			if (daysList.contains(day) && 
				!previousWindow.equalsIgnoreCase("previousWindowNotAvailable") && 
				bloomFilter.membershipTest(new Key(previousWindow.getBytes()))) {
				/**
				 * In else weight is assigned to 1 because if false positive
				 * occurs hashmap maynot contain value for that window.
				 */
				if (map.containsKey(previousWindow))
					weight = map.get(previousWindow) + 1;
				else
					weight = 1;

				if (!bloomFilter.membershipTest(new Key(window.getBytes())))
					bloomFilter.add(new Key(window.getBytes()));

				map.put(window, weight);
				records[0] = new Record(day, weight, hour);
				context.write(new Text(song), new RecordArrayWritable(records));

			} else if (daysList.contains(day) && 
					!bloomFilter.membershipTest(new Key(previousWindow.getBytes()))) {

				weight = 1;
				map.put(window, weight);
				records[0] = new Record(day, weight, hour);
				bloomFilter.add(new Key(window.getBytes()));
				context.write(new Text(song), new RecordArrayWritable(records));
			}

		}
	}
}
