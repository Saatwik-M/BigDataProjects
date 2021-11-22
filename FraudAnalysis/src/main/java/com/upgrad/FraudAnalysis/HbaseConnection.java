package com.upgrad.FraudAnalysis;

import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseConnection implements Serializable 
{ 
	private static final long serialVersionUID = 1L;
	static Admin hbaseAdmin = null; 
	public static Admin getHbaseAdmin(String awsUrl,String clientPort,String masterPort) throws IOException {
		org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration) HBaseConfiguration.create();
		conf.setInt("timeout", 1200); 
		conf.set("hbase.master", awsUrl+":"+masterPort); //60000
		conf.set("hbase.zookeeper.quorum", awsUrl); 
		conf.set("hbase.zookeeper.property.clientPort", clientPort); //2181
		conf.set("zookeeper.znode.parent", "/hbase"); 
		Connection con = ConnectionFactory.createConnection(conf); 
		try { 
			if (hbaseAdmin == null) //hbaseAdmin = new HBaseAdmin(conf); 
				hbaseAdmin = con.getAdmin();
			} catch (Exception e) 
		{ 
				e.printStackTrace(); 
				} 
		return hbaseAdmin; } 
	}