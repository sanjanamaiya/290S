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

public class SplitTemplate {

	private static final int CHUNK_SIZE = 50000;
	private static final String SOURCE_SELECT_QUERY = "select * from main.Tweets limit ?,? ";
	static final Logger LOG = LoggerFactory.getLogger(SplitTemplate.class);

	public void recreate(DataSplit strategy, SystemDetails sysDetails) throws SQLException {

		clearDataFromServers(sysDetails, strategy.getTargetTableName());
		runSplit(strategy, sysDetails);

	}

	public void runSplit(DataSplit strategy, SystemDetails systemDetails) throws SQLException {

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
				splitInChunks(strategy, hashedList, sourceConn, startLimit);

				// uncomment to debug:
				// printList(hashedList);

				distribute(systemDetails, hashedList, strategy.getTargetTableName());
				startLimit = startLimit + CHUNK_SIZE;

			}

		}
		LOG.info("Split completed in " + (System.nanoTime() - time));
	}

	private void splitInChunks(DataSplit strategy, List<ArrayList<TwitterStatus>> hashedList, Connection sourceConn,
			int startLimit) throws SQLException {

		try (PreparedStatement stmt = sourceConn.prepareStatement(SOURCE_SELECT_QUERY)) {
			stmt.setInt(1, startLimit);
			stmt.setInt(2, CHUNK_SIZE);
			ResultSet rs = stmt.executeQuery();
			TwitterStatus status = null;

			while (rs.next()) {
				status = new TwitterStatus();
				status.setTwitterStatusId(rs.getLong("TwitterStatusId"));
				status.setUserId(rs.getLong("UserId"));
				status.setUserScreenName(rs.getString("UserScreenName"));
				status.setText(rs.getString("Text"));
				status.setUserMentions(rs.getString("UserMentions"));
				status.setHashTags(rs.getString("HashTags"));
				int serverIndex = strategy.getServerIndex(status);
				hashedList.get(serverIndex).add(status);
			}

		} catch (SQLException e) {
			LOG.error("error retrieving users from source", e);
			throw e;
		}

	}

	private List<ArrayList<TwitterStatus>> setupHashedList(int serverCount) {
		List<ArrayList<TwitterStatus>> hashedList = new ArrayList<ArrayList<TwitterStatus>>();
		for (int index = 0; index < serverCount; index++) {
			hashedList.add(new ArrayList<TwitterStatus>());
		}
		return hashedList;
	}

	private void distribute(SystemDetails sysDetails, List<ArrayList<TwitterStatus>> tweets, String tableName)
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
	}

	private void batchWrite(Connection conn, ArrayList<TwitterStatus> statusList, String tableName)
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
