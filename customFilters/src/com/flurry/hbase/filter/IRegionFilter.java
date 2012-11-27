package com.flurry.hbase.filter;

public interface IRegionFilter
{
	public abstract boolean filterRegion(byte[] startRow, byte[] endRow);
}
