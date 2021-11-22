package com.trend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Record implements Writable {
	
	private String day; 
	private int weight;
	private int hour;
	
	public Record() {
	}
	
	public Record(String day, int weight , int hour) {
		this.day = day;
		this.weight = weight;
		this.hour = hour;
	}

	
	public int getHour() {
		return hour;
	}

	public void setHour(int hour) {
		this.hour = hour;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public void readFields(DataInput dataInput) throws IOException {
		day = WritableUtils.readString(dataInput);
		weight =WritableUtils.readVInt(dataInput);
		hour =WritableUtils.readVInt(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, day);
		WritableUtils.writeVInt(dataOutput, weight);
		WritableUtils.writeVInt(dataOutput, hour);
	}

}
