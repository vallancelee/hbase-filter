package com.flurry.hbase.filter;

import java.io.*;
import java.net.*;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Manages and loads the classes from a jar located in HDFS. For the purposes of this example,
 * only one jar is supported and the configured HDFS path is to the jar itself.
 */
public class HdfsJarModule
{	
	private static final Logger LOG = Logger.getLogger(HdfsJarModule.class.getName());
	
	private static final Path DEFAULT_TMP_DIR = new Path("/tmp/modules");
	
	private Path localTmpDir;
	private Path jarPath;
	private long jarTimestamp;
	private ClassLoader parentClassLoader;
	private ClassLoader moduleClassLoader;
	private File tmpLocal;

	/**
	 * @param jarPath The absolute HDFS path the jar
	 */
	public HdfsJarModule(Path jarPath, ClassLoader parentClassLoader)
	{
		this(jarPath, DEFAULT_TMP_DIR, parentClassLoader);
	}

	/**
	 * @param jarPath The absolute HDFS path the jar
	 * @param localDir local directory for storing the jars downloaded from HDFS
	 */
	public HdfsJarModule(Path jarPath, Path localDir, ClassLoader parentClassLoader)
	{
		this.jarPath = jarPath;
		this.localTmpDir = localDir;
		this.parentClassLoader = parentClassLoader;
	}
	
	public Class<?> getClass(String className) throws IOException, ClassNotFoundException
	{
		loadModule();
		return moduleClassLoader.loadClass(className);
	}
	
	/**
	 * Releases the resources used by the module. 
	 */
	public void unloadModule()
	{
		if (moduleClassLoader instanceof URLClassLoader)
		{
			/* 
			 * In Java 7, use ((URLClassLoader) moduleClassLoader).close() to release the jars 
			 * otherwise, things get a little more involved -- see 
			 * http://loracular.blogspot.com/2009/12/dynamic-class-loader-with.html
			 */
		}
		moduleClassLoader = null;
	}

	/**
	 * If the module has not been loaded, or if it has been modified since it was last
	 * loaded, copy the jar from HDFS to a local directory. Use the local copy for loading classes.
	 */
	private void loadModule() throws IOException
	{
		if (jarPath == null)
		{
			// undefined path, use the parent classLoader
			moduleClassLoader = parentClassLoader;
			return;
		}
		
		Configuration config = HBaseConfiguration.create();
		FileSystem fs = jarPath.getFileSystem(config);
		FileStatus fileStatus = fs.getFileStatus(jarPath);
		final long modificationTime = fileStatus.getModificationTime();
		
		boolean shouldInitialize = moduleClassLoader == null || modificationTime > jarTimestamp;
		
		LOG.info("should initialize? " + shouldInitialize);
		if (shouldInitialize)
		{		
			Path localPath = new Path(localTmpDir, System.currentTimeMillis() + "_" + jarPath.getName());
			fs.copyToLocalFile(jarPath, localPath);
			
			tmpLocal = new File(localPath.toString());
			tmpLocal.deleteOnExit();
			
			// unload the previous module
			unloadModule();
			
			final URL url = tmpLocal.toURI().toURL();
			
			java.security.AccessController.doPrivileged(new java.security.PrivilegedAction<String>()
			{
				@Override
				public String run()
				{
					moduleClassLoader = new ModuleFirstClassLoader(new URL[] { url }, parentClassLoader);
					jarTimestamp = modificationTime;
					return null;
				}
			});
			
		}
	}
}
