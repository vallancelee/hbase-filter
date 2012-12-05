package com.flurry.hbase.filter.example;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.flurry.hbase.filter.example.ExampleTable.ExampleRowKey;
import com.flurry.hbase.filter.impl.MyFilter;

/**
 * Some utility methods for the custom filter example.
 */

@SuppressWarnings("unused")
public class ExampleQuery
{	
	private static final Logger LOG = Logger.getLogger(ExampleQuery.class.getName());
	
	/**
	 * Query that expects the (the newer Filter is backward comp
	 */
	private static void doQuery() throws Exception
	{
		// test the filter as part of a filter list
		Scan scan = new Scan();
		FilterList filterList = new FilterList();
		filterList.addFilter(new MyFilter(1));
//		filterList.addFilter(new AppFilter(1, 163));
		filterList.addFilter(new SingleColumnValueFilter(
				ExampleTable.COLUMN_FAMILY_BYTES, ExampleTable.COLUMN_QUALIFIER, CompareOp.GREATER, Bytes.toBytes(319824283278001L)));
		scan.setFilter(filterList);
		
		int numScanned = 0;
		ResultScanner scanner = ExampleTable.getTable().getScanner(scan);
		for (Result result : scanner)
		{
			byte[] rowKeyBytes = result.getRow();
			ExampleRowKey rowKey = new ExampleRowKey();
			rowKey.readFields(new DataInputStream(new ByteArrayInputStream(rowKeyBytes)));
			KeyValue value = result.getColumnLatest(ExampleTable.COLUMN_FAMILY_BYTES, ExampleTable.COLUMN_QUALIFIER);
			LOG.fine(rowKey + ": " + Bytes.toLong(value.getValue()));
			numScanned++;
		}
		scanner.close();
		LOG.info("Scanned: " + numScanned);
	}
	
	/**
	 * Generate some test data for the example table.
	 */
	public static void generateData() throws Exception
	{	
		int numRows = 10000;
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
			put.add(ExampleTable.COLUMN_FAMILY_BYTES, ExampleTable.COLUMN_QUALIFIER, Bytes.toBytes(randValue));
			puts.add(put);
		}
		ExampleTable.getTable().put(puts);
		ExampleTable.getTable().flushCommits();
	}
	
	/**
	 * Move the filters jar to the default filters directory in HDFS for pickup by the
	 * custom filter classloader.
	 * 
	 * Assumes that the filters have been jarred in bin/com/flurry/hbase/filter/example/filters.jar
	 */
	private static void moveFiltersToHdfs() throws IOException
	{
		Configuration config = HBaseConfiguration.create();
		Path dest = new Path("/filters", "filters.jar");
		FileSystem fs = dest.getFileSystem(config);
		File file = new File("bin/com/flurry/hbase/filter/example", "filters.jar");
		Path src = new Path(file.getAbsolutePath());
		
		fs.copyFromLocalFile(src, dest);
	}
	
	public static void main(String[] args) throws Exception
	{
		ExampleQuery.doQuery();
	}
}
