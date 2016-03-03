package com.systems.s290.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.systems.s290.db.connection.MySQLDataSource;

public class StaticHashSplit implements DataSplit {

	static final Logger LOG = LoggerFactory.getLogger(DataSplit.class);
	
	public void split(SystemDetails systemDetails) throws SQLException {
		
		try(Connection source = MySQLDataSource.getConnection(systemDetails.getSourceConnectionString())) {
			
			int count = getTweetCount(source);
			System.out.println("Count is " + count);
			if (count == 0)
			{
				LOG.warn("Count from master db is 0, exiting");
				return;
			}
			
			String sqlString = "select * from Tweets limit ?,? ";
			int startLimit = 0;
			int size = 50000;
			
			while (startLimit < count)
			{
				System.out.println("StartLimit is " + startLimit);
				List<ArrayList<TwitterStatus>> hashedList = setupHashedList();
				try (PreparedStatement stmt = source.prepareStatement(sqlString))
				{
					stmt.setInt(1, startLimit);
					stmt.setInt(2, size);
					ResultSet rs = stmt.executeQuery();
					TwitterStatus status = null;
					while(rs.next())
					{
						status = new TwitterStatus();
						status.setTwitterStatusId(rs.getLong("TwitterStatusId"));
						status.setUserId(rs.getLong("UserId"));
						status.setUserScreenName(rs.getString("UserScreenName"));
						status.setText(rs.getString("Text"));
						status.setUserMentions(rs.getString("UserMentions"));
						status.setHashTags(rs.getString("HashTags"));
						int hashNumber = getHash(status.getUserId(), systemDetails.getServerCount());
						hashedList.get(hashNumber).add(status);
					}
				}
				catch(SQLException e)
				{
					LOG.error("error retrieving users from source", e);
					throw e;
				}
				
				// uncomment to debug:
				//printList(hashedList);
				
				distribute(systemDetails, hashedList);
				startLimit = startLimit + size;
			}
		}

	}
	

	/**
	 * A new server has been added/removed
	 * We reconfigure the entire system
	 * @param sysDetails
	 * @throws SQLException 
	 */
	public void recreate(SystemDetails sysDetails) throws SQLException
	{
		clearDataFromServers(sysDetails);
		split(sysDetails);
	}
	
	
	private void clearDataFromServers(SystemDetails sysDetails) throws SQLException 
	{
		List<String> connStrings = sysDetails.getTargetConnectionStrings();
		for (String connStr : connStrings)
		{
			// Connect to db and delete all entries
			try(Connection conn = MySQLDataSource.getConnection(connStr))
			{
				String sql = "delete from TweetsS";
				try (PreparedStatement stmt = conn.prepareStatement(sql))
				{
					stmt.executeUpdate();
				}
			}
		}
	}


	private int getTweetCount(Connection conn) 
	{
		int count = 0;
		String sql = "select count(*) from Tweets";
		try (Statement stmt = conn.createStatement())
		{
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			count  = rs.getInt(1);
		}
		catch(SQLException e)
		{
			LOG.warn("Error getting count from database: ", e);
		}
		return count;
	}
	
	private List<ArrayList<TwitterStatus>> setupHashedList() 
	{
		List<ArrayList<TwitterStatus>> hashedList = new ArrayList<ArrayList<TwitterStatus>>();
		hashedList.add(new ArrayList<TwitterStatus>());
		hashedList.add(new ArrayList<TwitterStatus>());
		hashedList.add(new ArrayList<TwitterStatus>());
		hashedList.add(new ArrayList<TwitterStatus>());
		hashedList.add(new ArrayList<TwitterStatus>());
		return hashedList;
	}

	private int getHash(long userId, int count) 
	{
		return Math.abs(Long.hashCode(userId)) % count;
	}
	
	private void distribute(SystemDetails sysDetails, List<ArrayList<TwitterStatus>> tweets) throws SQLException
	{
		int i = 0;
		for (ArrayList<TwitterStatus> statusList : tweets)
		{
			String connString = sysDetails.getTargetConnectionStrings().get(i);
			try (Connection conn = MySQLDataSource.getConnection(connString))
			{
				batchWrite(conn, statusList);
			}
			catch (SQLException e)
			{
				LOG.error("error creating db connection to " +  connString + ": ", e);
				throw e;
			}
			i++;
		}
	}
	
	
	private void batchWrite(Connection conn, ArrayList<TwitterStatus> statusList) throws SQLException 
	{
		String updateString = "insert into TweetsS values(?, ?, ?, ?, ?,?)";
		try (PreparedStatement stmt = conn.prepareStatement(updateString))
		{
			int j = 0;
			for (TwitterStatus twitterStatus : statusList)
			{
				stmt.setLong(1, twitterStatus.getTwitterStatusId());
				stmt.setLong(2, twitterStatus.getUserId());
				stmt.setString(3, twitterStatus.getUserScreenName());
				stmt.setString(4, twitterStatus.getText());
				stmt.setString(5, twitterStatus.getUserMentions());
				stmt.setString(6, twitterStatus.getHashTags());
				stmt.addBatch();
				j++;
				
				if ((j % 1000 == 0) || (j == statusList.size()))
				{
					stmt.executeBatch();
				}
			}
		}
		catch (SQLException e) 
		{
			LOG.warn("error inserting rows: ", e);
			throw e;
		}
	}
	
	// Testing
	public static void main(String[] args)
	{
		SystemDetails details = new SystemDetails();
		details.setColumnCount(6);
		List<String> connectionStrings = new ArrayList<String>();
		connectionStrings.add("instance290-1.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306/main");
		connectionStrings.add("instance290-2.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306/main");
		connectionStrings.add("instance290-3.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306/main");
		connectionStrings.add("instance290-4.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306/main");
		connectionStrings.add("instance290-5.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306/main");
		details.setConnectionStrings(connectionStrings);
		details.setSourceConnectionString("instance290-0.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306/main");
		
		DataSplit split = new StaticHashSplit();
		try {
			split.recreate(details);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
