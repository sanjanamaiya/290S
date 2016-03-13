package com.systems.s290.data;

import java.util.List;

public class SystemDetails {

	private List<String> targetConnectionStrings;
	private String sourceConnectionString;
	private List<String> distributedDirConnStrings; 	

	public List<String> getTargetConnectionStrings() {
		return targetConnectionStrings;
	}

	public void setTargetConnectionStrings(List<String> targetConnectionStrings) {
		this.targetConnectionStrings = targetConnectionStrings;
	}
	
	public List<String> getDistributedDirConnStrings() {
		return distributedDirConnStrings;
	}

	public void setDistributedDirConnStrings(List<String> targetConnectionStrings) {
		this.distributedDirConnStrings = targetConnectionStrings;
	}
	
	// TODO this needs to be set based on application conf
	public int getServerCount()
	{
		return targetConnectionStrings.size();
	}
	
	public int getDistributedDirCount()
	{
		return (distributedDirConnStrings == null) ? 0 : distributedDirConnStrings.size();
	}

	public String getSourceConnectionString() {
		return sourceConnectionString;
	}

	public void setSourceConnectionString(String sourceConnectionString) {
		this.sourceConnectionString = sourceConnectionString;
	}

}
