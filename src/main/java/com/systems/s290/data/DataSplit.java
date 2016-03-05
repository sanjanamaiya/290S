package com.systems.s290.data;

public interface DataSplit {
	
	public int getServerIndex(TwitterStatus status);
	public String getTargetTableName();

}
