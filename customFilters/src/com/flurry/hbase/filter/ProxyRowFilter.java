package com.flurry.hbase.filter;

import java.io.*;
import java.util.List;
import java.util.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.filter.Filter;

public class ProxyRowFilter implements Filter
{
	private static final Logger LOG = Logger.getLogger(ProxyRowFilter.class.getName());
	
	// HDFS path to the jar containing the filters
	private static final String HDFS_FILTER_JAR = "flurry.filter.jar";
	private static String filtersPath;
	
	private static HdfsJarModule module;	
	private Filter filter;
	
	public ProxyRowFilter() 
	{
		// initialized server-side
		init();
	}
	
	public ProxyRowFilter(Filter filter)
	{
		this.filter = filter;
	}
	
	@Override
	public void reset()
	{
		filter.reset();
	}

	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length)
	{
		return filter.filterRowKey(buffer, offset, length);
	}

	@Override
	public boolean filterAllRemaining()
	{
		return filter.filterAllRemaining();
	}

	@Override
	public ReturnCode filterKeyValue(KeyValue ignored)
	{
		return filter.filterKeyValue(ignored);
	}

	@Override
	public KeyValue transform(KeyValue v)
	{
		return filter.transform(v);
	}

	@Override
	public void filterRow(List<KeyValue> ignored)
	{
		filter.filterRow(ignored);
	}

	@Override
	public boolean hasFilterRow()
	{
		return filter.hasFilterRow();
	}

	@Override
	public boolean filterRow()
	{
		return filter.filterRow();
	}

	@Override
	public KeyValue getNextKeyHint(KeyValue currentKV)
	{
		return filter.getNextKeyHint(currentKV);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		String filterName = filter.getClass().getName();
		LOG.info("filtername: " + filterName);
		out.writeUTF(filterName);
		filter.write(out);
	}

	@Override
	public void readFields(final DataInput in) throws IOException
	{
		final String className = in.readUTF();
		LOG.info("Read name: " + className);
		try
		{
			LOG.info("Getting class: " + className);
			Class<?> filterClass = module.getClass(className);

			LOG.info("Instantiating class: " + className);
			filter = (Filter) filterClass.newInstance();
			
			LOG.info("Initializing class: " + className);
			filter.readFields(in);
			
			LOG.info("Done reading class");
		}
		catch (ClassNotFoundException e)
		{
			LOG.severe("Class " + className + " not found");
		}
		catch (InstantiationException e)
		{
			LOG.severe("Cannot instantiate class " + className);
		}
		catch (IllegalAccessException e)
		{
			LOG.severe("Not allowed to instantiate class " + className);
		}
		catch (IOException e)
		{
			LOG.log(Level.SEVERE, "Deserialization error", e);
		}
	}
	
	public static void unloadModule()
	{
		if (module != null)
		{
			module.unloadModule();
		}
	}

	private static void init()
	{
		if (module == null)
		{
			Configuration config = HBaseConfiguration.create();
			filtersPath = config.get(HDFS_FILTER_JAR, null);
	
			LOG.info("Row filters path: " + filtersPath);
			Path path = filtersPath == null ? null : new Path(filtersPath);
			module = new HdfsJarModule(path);
			LOG.info("Loaded HdfsJarModule");
		}
	}

}
