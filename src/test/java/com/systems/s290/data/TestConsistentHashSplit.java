package com.systems.s290.data;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class TestConsistentHashSplit  {

	
	@Test
	public void testConsistentHash() throws SQLException{
		SystemDetails details = new SystemDetails();
		details.setColumnCount(6);
		List<String> connectionStrings = new ArrayList<String>();
		connectionStrings.add("instance290-1.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		connectionStrings.add("instance290-2.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		connectionStrings.add("instance290-3.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		connectionStrings.add("instance290-4.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		connectionStrings.add("instance290-5.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		details.setConnectionStrings(connectionStrings);
		details.setInsertQuery("INSERT INTO main.TweetsC"
				+ "(TwitterStatusId, UserId, UserScreenName, Text, UserMentions, HashTags) VALUES" + "(?,?,?,?,?,?)");
		details.setKey("UserId");
		details.setSourceConnectionString("instance290-0.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		details.setSelectQuery("select * from main.Tweets");
		DataSplit split = new ConsistentHashSplit();
		split.split(details);
	}
	

}
