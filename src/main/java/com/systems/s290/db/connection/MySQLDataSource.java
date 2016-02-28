package com.systems.s290.db.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;

public class MySQLDataSource {

	private static Map<String, BasicDataSource> dataSourceMap = new HashMap<String, BasicDataSource>();

	private static BasicDataSource getInstance(String connectionString) {
		if(dataSourceMap.get(connectionString) == null){
			 BasicDataSource ds = new BasicDataSource();
			 ds.setDriverClassName("com.mysql.jdbc.Driver");
		     ds.setUsername("usersg");
		     ds.setPassword("passwordsg");
		     ds.setUrl("jdbc:mysql://"+connectionString);
		     dataSourceMap.put(connectionString, ds);
		}
		 
		return dataSourceMap.get(connectionString);
	     
	}
	
	public static Connection getConnection(String connectionString) throws SQLException {
		return getInstance(connectionString).getConnection();
	}

}
