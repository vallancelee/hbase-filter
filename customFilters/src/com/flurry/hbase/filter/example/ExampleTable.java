package com.flurry.hbase.filter.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class ExampleTable
{
	public static final String tableName = "applicationEvents";
	public static final String COLUMN_FAMILY = "v";
	public static final byte[] COLUMN_FAMILY_BYTES = COLUMN_FAMILY.getBytes();
	public static final byte[] COLUMN_QUALIFIER = new byte[] {0};
	private static final Configuration config = HBaseConfiguration.create();
	
	public static HTable getTable() throws Exception
	{
		return new HTable(config, Bytes.toBytes(tableName));
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
		ExampleTable.createTable();
	}
}
