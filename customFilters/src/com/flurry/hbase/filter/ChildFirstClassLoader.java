package com.flurry.hbase.filter;

import java.net.*;
import java.util.Arrays;
import java.util.logging.Logger;

public class ChildFirstClassLoader extends URLClassLoader
{
	private static Logger LOG = Logger.getLogger(ChildFirstClassLoader.class.getName());
	
	public ChildFirstClassLoader(URL[] urls)
	{
		super(urls);
		LOG.info("Initialized with urls: " + Arrays.toString(urls));
	}

	@Override
	protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException
	{
        Class<?> c = findLoadedClass(name);

		if (c == null)
		{
            try
            {
                c = findClass(name);

    			LOG.info("Class found at module level: " + name);
    			
        		if (resolve)
        		{
        		    resolveClass(c);
        		}
            }
            catch (ClassNotFoundException e)
            {
            }

            if (c == null)
            {
                c = super.loadClass(name, resolve);
                LOG.info("Class found at parent level: " + name);
            }
        }
		else
		{
			LOG.info("Using loaded class: " + name);
		}

		return c;
    }
}
