package com.flurry.hbase.filter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HdfsJarModule
{	
	private static final Logger LOG = Logger.getLogger(HdfsJarModule.class.getName());
	
	private String localModulePath = "/tmp/modules";
	private Path jarPath;
	private long jarTimestamp;
	private ClassLoader moduleClassLoader;

	/**
	 * Manages and loads the classes from a jar located in HDFS.
	 */
	public HdfsJarModule(Path jarPath)
	{
		this.jarPath = jarPath;
	}
	
	public Class<?> getClass(String className) throws IOException, ClassNotFoundException
	{
		loadModule();
		return moduleClassLoader.loadClass(className);
	}
	
	public void unloadModule()
	{
		moduleClassLoader = null;
	}

	private void loadModule() throws IOException
	{
		if (jarPath == null)
		{
			moduleClassLoader = getClass().getClassLoader();
			return;
		}
		
		Configuration config = HBaseConfiguration.create();
		FileSystem fs = jarPath.getFileSystem(config);
		FileStatus fileStatus = fs.getFileStatus(jarPath);
		final long modificationTime = fileStatus.getModificationTime();
		
		boolean shouldInitialize = moduleClassLoader == null || modificationTime > jarTimestamp;
		
		LOG.info("shouldInitialize? " + shouldInitialize);
		if (shouldInitialize)
		{
			// use the hbase local tmp dir
			localModulePath = config.get("java.io.tmpdir", localModulePath);
			Path localPath = new Path(localModulePath, System.currentTimeMillis() + "_" + jarPath.getName());
			fs.copyToLocalFile(jarPath, localPath);
			
			LOG.info("Copied jar at " + jarPath + " to " + localPath);
			
			File tmpLocal = new File(localPath.toString(), "example.jar");
			tmpLocal.deleteOnExit();
			
			final URL url = tmpLocal.toURI().toURL();
			
			java.security.AccessController.doPrivileged(new java.security.PrivilegedAction<String>()
			{
				@Override
				public String run()
				{
					moduleClassLoader = new ChildFirstClassLoader(new URL[] { url });
					jarTimestamp = modificationTime;
					return null;
				}
			});
			
		}
	}
}
