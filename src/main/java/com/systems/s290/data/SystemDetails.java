package com.systems.s290.data;

import java.util.List;

public class SystemDetails {

	private List<String> targetConnectionStrings;
	private String sourceConnectionString;

	public List<String> getTargetConnectionStrings() {
		return targetConnectionStrings;
	}

	public void setTargetConnectionStrings(List<String> targetConnectionStrings) {
		this.targetConnectionStrings = targetConnectionStrings;
	}
	
	// TODO this needs to be set based on application conf
	public int getServerCount()
	{
		return targetConnectionStrings.size();
	}
	
	

	public List<String> getConnectionStrings() {
		return targetConnectionStrings;
	}

	public void setConnectionStrings(List<String> connectionStrings) {
		this.targetConnectionStrings = connectionStrings;
	}

	public String getSourceConnectionString() {
		return sourceConnectionString;
	}

	public void setSourceConnectionString(String sourceConnectionString) {
		this.sourceConnectionString = sourceConnectionString;
	}

}
