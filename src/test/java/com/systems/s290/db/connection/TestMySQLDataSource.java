package com.systems.s290.db.connection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

import com.systems.s290.db.connection.MySQLDataSource;

public class TestMySQLDataSource {

	@Test
	public void testconnection() throws SQLException{
		Connection conn = MySQLDataSource.getConnection("instance290-0.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		Assert.assertNotNull(conn);
		ResultSet result = conn.createStatement().executeQuery("select count(*) from main.Tweets");
		if(result.next()){
			Assert.assertTrue(result.getInt(1) >= 0);
		}
	}

}
