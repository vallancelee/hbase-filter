package com.flurry.hbase.filter;

import java.io.*;
import java.util.List;
import java.util.logging.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * A starting point for creating dynamically-loaded custom filters. 
 *
 */
public class ProxyFilter implements Filter, Configurable
{
	private static final Logger LOG = Logger.getLogger(ProxyFilter.class.getName());
	
	// HDFS path to the jar containing the filters
	private static final String HDFS_FILTER_JAR = "flurry.hdfs.filter.jar";
	// local tmp dir
	private static final String LOCAL_FILTER_DIR = "flurry.local.filter.dir";

	// default values
	private static final String DEFAULT_HDFS_FILTER_JAR_VALUE = "/flurry/filters.jar";
	private static final String DEFAULT_LOCAL_FILTER_DIR_VALUE = "/tmp/filters";
	
	private static String filtersPath;
	private static String localTmpDir;
	
	private static HdfsJarModule filterModule;	
	static 
	{
		init();
	}
	
	private Filter filter;

	public ProxyFilter() 
	{
	}
	
	public ProxyFilter(Filter filter)
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
		out.writeUTF(filterName);
		filter.write(out);
	}

	@Override
	public void readFields(final DataInput in) throws IOException
	{
		final String className = in.readUTF();
		LOG.info("Filter name: " + className);
		try
		{
			LOG.info("Getting class: " + className);
			Class<?> filterClass = filterModule.getClass(className);

			LOG.info("Instantiating class: " + className);
			filter = (Filter) filterClass.newInstance(); // also see WritableFactories.newInstance
			
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
	
	public static void cleanUp()
	{
		if (filterModule != null)
		{
			filterModule.unloadModule();
		}
	}

	private static void init()
	{
		Configuration config = HBaseConfiguration.create();
		filtersPath = config.get(HDFS_FILTER_JAR, DEFAULT_HDFS_FILTER_JAR_VALUE);
		localTmpDir = config.get(LOCAL_FILTER_DIR, DEFAULT_LOCAL_FILTER_DIR_VALUE);
		LOG.info("Filters path: " + filtersPath + ", local path: " + localTmpDir);
		
		Path path = filtersPath == null ? null : new Path(filtersPath);
		
		// we want to keep this simple and let the classloader that loaded 
		// the proxy handle standard classes
		ClassLoader parent = ProxyFilter.class.getClassLoader();
		filterModule = new HdfsJarModule(path, parent);
		LOG.info("Loaded HdfsJarModule");
	}

	@Override
	public void setConf(Configuration conf)
	{
		if (filter instanceof Configurable)
		{
			((Configurable) filter).setConf(conf);
		}
	}

	@Override
	public Configuration getConf()
	{
		return (filter instanceof Configurable) ? ((Configurable) filter).getConf() : null;
	}

}
