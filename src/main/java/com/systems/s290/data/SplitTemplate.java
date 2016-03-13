package com.systems.s290.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.systems.s290.db.connection.MySQLDataSource;

public class SplitTemplate {

	protected static final int CHUNK_SIZE = 50000;
	protected static final String SOURCE_SELECT_QUERY = "select * from main.Tweets limit ?,? ";
	static final Logger LOG = LoggerFactory.getLogger(SplitTemplate.class);

	public void recreate(HashingStrategy strategy, SystemDetails sysDetails) throws SQLException {

		clearDataFromServers(sysDetails, strategy.getTargetTableName());
		runSplit(strategy, sysDetails);

	}

	public void runSplit(HashingStrategy strategy, SystemDetails systemDetails) throws SQLException {

		LOG.info("Begin split with strategy " + strategy.getClass());
		long time = System.nanoTime();

		try (Connection sourceConn = MySQLDataSource.getConnection(systemDetails.getSourceConnectionString())) {

			// Check source data availability
			int sourceTweetCount = getTweetCount(sourceConn);
			System.out.println("Count is " + sourceTweetCount);
			if (sourceTweetCount == 0) {
				LOG.warn("Count from master db is 0, exiting");
				return;
			}

			// get data in chunks
			int startLimit = 0;
			while (startLimit < sourceTweetCount) {
				List<ArrayList<TwitterStatus>> hashedList = setupHashedList(systemDetails.getServerCount());
				List<HashMap<Long, String>> hashedDirList = setupHashedDirList(systemDetails.getDistributedDirCount());
				splitInChunks(strategy, hashedList, hashedDirList, sourceConn, startLimit, systemDetails);

				// uncomment to debug:
				// printList(hashedList);

				distribute(systemDetails, hashedList, hashedDirList, strategy.getTargetTableName(), strategy.getDistributedDirTableName());
				startLimit = startLimit + CHUNK_SIZE;

			}

		}
		LOG.info("Split completed in " + (System.nanoTime() - time));
	}

	private void splitInChunks(HashingStrategy strategy, List<ArrayList<TwitterStatus>> hashedList, List<HashMap<Long, String>> hashedDirList, Connection sourceConn,
			int startLimit, SystemDetails systemDetails) throws SQLException {

		try (PreparedStatement stmt = sourceConn.prepareStatement(SOURCE_SELECT_QUERY)) {
			stmt.setInt(1, startLimit);
			stmt.setInt(2, CHUNK_SIZE);
			ResultSet rs = stmt.executeQuery();
			TwitterStatus status = null;

			while (rs.next()) {
				status = setTwitterStatusDetails(rs);
				int serverIndex = strategy.getServerIndex(status);
				
				// TODO this is a hack, need to set this up
				if (strategy instanceof DistributedDirectoryStrategy && systemDetails.getDistributedDirCount() > 0)
				{
					// Use the staticStrategy to pick a server where this tweet will be saved
					StaticHashStrategy staticStr = new StaticHashStrategy(systemDetails);
					int tweetServerIndex = staticStr.getServerIndex(status);
					String connString = systemDetails.getTargetConnectionStrings().get(tweetServerIndex);
					
					// Place the mapping from UserId to server in hashedDirList
					// Place the Tweet for the UserId in the server indicated by the mapping
					hashedDirList.get(serverIndex).put(status.getUserId(), connString);
					serverIndex = tweetServerIndex;
					
				}
				
				hashedList.get(serverIndex).add(status);
			}

		} catch (SQLException e) {
			LOG.error("error retrieving users from source", e);
			throw e;
		}

	}

	public static TwitterStatus setTwitterStatusDetails(ResultSet rs)
			throws SQLException {
		TwitterStatus status;
		status = new TwitterStatus();
		status.setTwitterStatusId(rs.getLong("TwitterStatusId"));
		status.setUserId(rs.getLong("UserId"));
		status.setUserScreenName(rs.getString("UserScreenName"));
		status.setText(rs.getString("Text"));
		status.setUserMentions(rs.getString("UserMentions"));
		status.setHashTags(rs.getString("HashTags"));
		return status;
	}

	private List<ArrayList<TwitterStatus>> setupHashedList(int serverCount) {
		List<ArrayList<TwitterStatus>> hashedList = new ArrayList<ArrayList<TwitterStatus>>();
		for (int index = 0; index < serverCount; index++) {
			hashedList.add(new ArrayList<TwitterStatus>());
		}
		return hashedList;
	}
	
	private List<HashMap<Long, String>> setupHashedDirList(
			int distributedDirCount) {
		
		List<HashMap<Long, String>> hashedList = new ArrayList<HashMap<Long, String>>();
		for (int index = 0; index < distributedDirCount; index++) {
			hashedList.add(new HashMap<Long, String>());
		}
		return hashedList;
	}

	private void distribute(SystemDetails sysDetails, 
			List<ArrayList<TwitterStatus>> tweets, 
			List<HashMap<Long, String>> hashedDirList, 
			String tableName, 
			String distrTableName)
			throws SQLException {
		
		int i = 0;
		for (ArrayList<TwitterStatus> statusList : tweets) {
			String connString = sysDetails.getTargetConnectionStrings().get(i);
			try (Connection conn = MySQLDataSource.getConnection(connString)) {
				batchWrite(conn, statusList, tableName);
			} catch (SQLException e) {
				LOG.error("error creating db connection to " + connString + ": ", e);
				throw e;
			}
			i++;
		}
		
		int j = 0;
		for (HashMap<Long, String> distDirMap : hashedDirList)
		{
			if (distDirMap != null && distDirMap.size() > 0)
			{
				String connString = sysDetails.getDistributedDirConnStrings().get(j);
				try (Connection conn = MySQLDataSource.getConnection(connString);
						Connection conn_source = MySQLDataSource.getConnection(sysDetails.getSourceConnectionString())) {
					batchWrite(conn, conn_source, distDirMap, distrTableName);
				} catch (SQLException e) {
					LOG.error("error creating db connection to " + connString + ": ", e);
					throw e;
				}
			}
			j++;
		}
		
		
	}

	/**
	 * Write to DistributedUserHash in the current server and the source server
	 * May need to separate out the two for re-usability
	 * @param conn
	 * @param conn_source
	 * @param distDirMap
	 * @param sourceConnection 
	 * @throws SQLException 
	 */
	private void batchWrite(Connection conn, Connection conn_source,
			HashMap<Long, String> distDirMap, String distrTableName) throws SQLException {
		
		String updateString = "insert into main." + distrTableName + " values(?, ?)";
		try (PreparedStatement stmt = conn.prepareStatement(updateString)) {
			int j = 0;
			for (Long twitterId : distDirMap.keySet()) {
				stmt.setLong(1, twitterId);
				stmt.setString(2, distDirMap.get(twitterId));
				stmt.addBatch();
				j++;

				if ((j % 1000 == 0) || (j == distDirMap.size())) {
					stmt.executeBatch();
				}
			}
		} catch (SQLException e) {
			LOG.warn("error inserting rows into server : ", e);
			throw e;
		}
		
		if (conn_source != null)
		{
			try (PreparedStatement stmt = conn_source.prepareStatement(updateString)) {
				int j = 0;
				for (Long twitterId : distDirMap.keySet()) {
					stmt.setLong(1, twitterId);
					stmt.setString(2, distDirMap.get(twitterId));
					stmt.addBatch();
					j++;

					if ((j % 1000 == 0) || (j == distDirMap.size())) {
						stmt.executeBatch();
					}
				}
			} catch (SQLException e) {
				LOG.warn("error inserting rows into source server: ", e);
				throw e;
			}
		}
	}

	public static void batchWrite(Connection conn, List<TwitterStatus> statusList, String tableName)
			throws SQLException {
		String updateString = "insert into main." + tableName + " values(?, ?, ?, ?, ?,?)";
		try (PreparedStatement stmt = conn.prepareStatement(updateString)) {
			int j = 0;
			for (TwitterStatus twitterStatus : statusList) {
				stmt.setLong(1, twitterStatus.getTwitterStatusId());
				stmt.setLong(2, twitterStatus.getUserId());
				stmt.setString(3, twitterStatus.getUserScreenName());
				stmt.setString(4, twitterStatus.getText());
				stmt.setString(5, twitterStatus.getUserMentions());
				stmt.setString(6, twitterStatus.getHashTags());
				stmt.addBatch();
				j++;

				if ((j % 1000 == 0) || (j == statusList.size())) {
					stmt.executeBatch();
				}
			}
		} catch (SQLException e) {
			LOG.warn("error inserting rows: ", e);
			throw e;
		}
	}

	private int getTweetCount(Connection conn) {
		int count = 0;
		String sql = "select count(*) from main.Tweets";
		try (Statement stmt = conn.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			count = rs.getInt(1);
		} catch (SQLException e) {
			LOG.warn("Error getting count from database: ", e);
		}
		return count;
	}

	private void clearDataFromServers(SystemDetails sysDetails, String tableName) throws SQLException {
		LOG.info("Clearing data from all targets");
		List<String> connStrings = sysDetails.getTargetConnectionStrings();
		for (String connStr : connStrings) {
			// Connect to db and delete all entries
			try (Connection conn = MySQLDataSource.getConnection(connStr)) {
				String sql = "delete from main." + tableName;
				try (PreparedStatement stmt = conn.prepareStatement(sql)) {
					stmt.executeUpdate();
				}
			}
		}
	}

}
