package com.flurry.hbase.filter.example;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class ExampleTable
{
	private static final String tableName = "exampleTable";
	public static final String COLUMN_FAMILY = "v";
	public static final byte[] COLUMN_FAMILY_BYTES = COLUMN_FAMILY.getBytes();
	public static final byte[] COLUMN_QUALIFIER = new byte[] {0};
	private static final Configuration config = HBaseConfiguration.create();
	
	public static HTable getTable() throws Exception
	{
		return new HTable(config, tableName.getBytes());
	}
	
	public static void createTable() throws Exception
	{
		HBaseAdmin admin = new HBaseAdmin(config);
		
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		HColumnDescriptor colFam = new HColumnDescriptor(COLUMN_FAMILY);
		colFam.setMaxVersions(1);
		descriptor.addFamily(colFam);
		
		admin.createTable(descriptor);
	}
	
	public static void generateData() throws Exception
	{	
		int numRows = 1000;
		long current = System.currentTimeMillis();
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < numRows; i++)
		{
			long randAppId = (long) (Math.random() * 100) + 1; // between 1 to 100
			long randVersionId = (long) (Math.random() * 100) + 101; // 101 to 200
			long randTs = (long) (Math.random() * current); // epoch 0 to now
			long randValue = (long) (Math.random() * current) * 1000; // 0 to 999
			
			ExampleRowKey rowKey = new ExampleRowKey(randAppId, randVersionId, randTs);
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(byteStream);
			rowKey.write(out);
			
			byte[] rowKeyBytes = byteStream.toByteArray();
			Put put = new Put(rowKeyBytes);
			put.add(COLUMN_FAMILY_BYTES, COLUMN_QUALIFIER, Bytes.toBytes(randValue));
			puts.add(put);
		}
		getTable().put(puts);
		getTable().flushCommits();
	}
	
	public static class ExampleRowKey implements Writable
	{
		private long appId;
		private long versionId;
		private long ts;
		
		public ExampleRowKey()
		{
		}

		public ExampleRowKey(long appId, long versionId, long ts)
		{
			this.appId = appId;
			this.versionId = versionId;
			this.ts = ts;
		}
		
		
		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeLong(appId);
			out.writeLong(versionId);
			out.writeLong(ts);
		}

		@Override
		public void readFields(DataInput in) throws IOException
		{
			appId = in.readLong();
			versionId = in.readLong();
			ts = in.readLong();
		}
		
		@Override
		public String toString()
		{
			StringBuilder sb = new StringBuilder();
			sb.append("{appId:").append(appId);
			sb.append(",versionId:").append(versionId);
			sb.append(",ts:" + ts).append("}");
			return sb.toString();
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		ExampleTable.generateData();
	}
}
