package com.flurry.hbase.filter.impl;

import java.io.*;

import org.apache.hadoop.hbase.filter.FilterBase;

/** 
 * The older version of MyFilter that is deployed in the cluster.
 *
 */
public class MyOldFilter extends FilterBase
{
	private long appId; 
	
	public MyOldFilter() {}
	
	public MyOldFilter(long appId)
	{
		this.appId = appId;
	}
	
	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length) 
	{		
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer, offset, length));
		try
		{
			long currentAppId = in.readLong();
			return currentAppId != appId;
		}
		catch (IOException e)
		{
		}
		finally
		{
			try
			{
				in.close();
			}
			catch (IOException e)
			{
			}
		}
		return true;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(appId);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		appId = in.readLong();
	}

}
