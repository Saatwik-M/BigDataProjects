package com.trend;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RecordArrayWritable extends ArrayWritable {
        public RecordArrayWritable() {
            super(Record.class);
        }
        /**
         * To Store Array of object Record.
         * @param dayWiseCountArr
         */
        public RecordArrayWritable(Record[] dayWiseCountArr) {
            super(Record.class);
            Record[] dayWiseCounts = new Record[dayWiseCountArr.length];
            for (int i = 0; i < dayWiseCountArr.length; i++) {
            	dayWiseCounts[i] = dayWiseCountArr[i];
            }
            set(dayWiseCounts);
        }
        
        public Writable[] getRecordArrayWritable()
        {
        	return get();
        	
        }
        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();

            for(String s : super.toStrings()){
                sb.append(s).append("\t");
            }

            return sb.toString();
        }
    }
