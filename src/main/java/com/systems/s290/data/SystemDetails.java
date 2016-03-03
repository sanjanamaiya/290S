package com.systems.s290.data;

import java.util.List;

public class SystemDetails {

	private List<String> targetConnectionStrings;
	private String sourceConnectionString;
	private String key;
	private String selectQuery;
	private String insertQuery;
	private int columnCount;

	public List<String> getTargetConnectionStrings() {
		return targetConnectionStrings;
	}

	public void setTargetConnectionStrings(List<String> targetConnectionStrings) {
		this.targetConnectionStrings = targetConnectionStrings;
	}
	
	// TODO this needs to be set based on application conf
	public int getServerCount()
	{
		return 5;
	}
	
	// TODO
	public void setServerCount()
	{
		
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getSelectQuery() {
		return selectQuery;
	}

	public void setSelectQuery(String selectQuery) {
		this.selectQuery = selectQuery;
	}

	public String getInsertQuery() {
		return insertQuery;
	}

	public void setInsertQuery(String insertQuery) {
		this.insertQuery = insertQuery;
	}

	public int getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(int columnCount) {
		this.columnCount = columnCount;
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
