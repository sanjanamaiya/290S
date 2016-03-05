package com.systems.s290.data;

public interface HashingStrategy {
	
	public int getServerIndex(TwitterStatus status);
	public String getTargetTableName();
}
