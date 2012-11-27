package com.flurry.hbase.filter.example;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import com.flurry.hbase.filter.example.ExampleTable.ExampleRowKey;

public class ExampleQuery
{	
	static void doQuery() throws Exception
	{
		// test the filter
		Scan scan = new Scan();
		FilterList filterList = new FilterList();
		filterList.addFilter(new AppFilter(1, 163));
		filterList.addFilter(new SingleColumnValueFilter(
				ExampleTable.COLUMN_FAMILY_BYTES, ExampleTable.COLUMN_QUALIFIER, CompareOp.GREATER, Bytes.toBytes(319824283278001L)));
		scan.setFilter(filterList);
		
		int numScanned = 0;
		ResultScanner results = ExampleTable.getTable().getScanner(scan);
		for (Result result : results)
		{
			byte[] rowKeyBytes = result.getRow();
			ExampleRowKey rowKey = new ExampleRowKey();
			rowKey.readFields(new DataInputStream(new ByteArrayInputStream(rowKeyBytes)));
			KeyValue value = result.getColumnLatest(ExampleTable.COLUMN_FAMILY_BYTES, ExampleTable.COLUMN_QUALIFIER);
			System.out.println(rowKey + ": " + Bytes.toLong(value.getValue()));
			numScanned++;
		}
		System.out.println("Scanned: " + numScanned);
	}
	
	static void moveFiltersToHdfs() throws IOException
	{
		Configuration config = HBaseConfiguration.create();
		Path dest = new Path("/filters/example.jar");
		FileSystem fs = dest.getFileSystem(config);
		File file = new File("bin/com/flurry/hbase/filter/example/example.jar");
		Path src = new Path(file.getAbsolutePath());
		System.out.println("Current dir: " + file.getAbsolutePath() + ", exists: " + file.exists());
		
		fs.copyFromLocalFile(src, dest);
	}
	
	public static void main(String[] args) throws Exception
	{
		ExampleQuery.doQuery();
	}
}
