package com.flurry.hbase.filter.example;

import java.io.*;

import org.apache.hadoop.hbase.filter.FilterBase;

public class AppFilter extends FilterBase
{
	private long appId; 
	private long versionId;	// optional, 0 if unset
	private boolean skipRest;
	
	public AppFilter() {}
	
	public AppFilter(long appId)
	{
		this(appId, 0);
	}
	
	public AppFilter(long versionId, long appId)
	{
		this.appId = appId;
		this.versionId = versionId;
	}
	
	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length) 
	{
		boolean skipThisRow = false;
		
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer, offset, length));
		try
		{
			long currentAppId = in.readLong();
			if (currentAppId == appId)
			{
				if (versionId > 0)
				{
					// need to check for the version id
					long currentVersionId = in.readLong();
					if (currentVersionId == versionId)
					{
						// app and version ids match 
						skipThisRow = false;
					}
					else if (currentVersionId != versionId)
					{
						skipThisRow = true;
						skipRest = currentVersionId > versionId;
					}
				}
				else
				{
					// matches app id
					skipThisRow = false;
				}
			}
			else if (currentAppId > 0)
			{
				skipRest = true;
			}
		}
		catch (IOException e)
		{
			skipThisRow = true;
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
		return skipThisRow;
	}

	@Override
	public boolean filterAllRemaining()
	{
		return skipRest;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(versionId);
		out.writeLong(appId);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		appId = in.readLong();
		versionId = in.readLong();
	}

}
