package com.systems.s290.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.systems.s290.db.connection.MySQLDataSource;

public class DumpData {

	public static void main(String[] args) throws IOException, SQLException {

		dumpData();

	}

	private static void dumpData() throws SQLException,  IOException{
		BufferedReader reader = new BufferedReader(new FileReader(new File("output_user_agrestamastelj.output")));
		String line = null;
		Connection conn = MySQLDataSource.getConnection("instance290-0.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		String insertTableSQL = "INSERT INTO main.Tweets"
				+ "(TwitterStatusId, UserId, UserScreenName, Text, UserMentions, HashTags) VALUES" + "(?,?,?,?,?,?)";
		PreparedStatement preparedStatement = conn.prepareStatement(insertTableSQL);

		int count = 0;

		while ((line = reader.readLine()) != null) {
			String[] columns = line.split(",");
			if (columns.length >= 6) {

				count++;
				preparedStatement.setLong(1, Long.parseLong(columns[0]));
				Long bi = Long.parseLong(columns[1]); // max unsigned 64-bit
														// number
				preparedStatement.setString(2, bi.toString());
				preparedStatement.setString(3, columns[2]);
				preparedStatement.setString(4, columns[3]);
				preparedStatement.setString(5, getUserMentions(columns));
				preparedStatement.setString(6, columns[columns.length - 1]);

				execute(preparedStatement);

			}

			if (count != 0) {
				execute(preparedStatement);
			}

		}

		reader.close();
		preparedStatement.close();
		conn.close();
		// writer.close();
	}
	
	private static void execute(PreparedStatement preparedStatement) {
		try {

			preparedStatement.execute();

		} catch (Exception e) {
			System.out.println("In exception:" + e.getMessage());
		}
	}

	private static String getUserMentions(String[] columns) {
		String userMentions = "";

		for (int i = 4; i <= columns.length - 2; i++) {
			if (i < columns.length)
				userMentions += columns[i] + ",";
		}
		if (userMentions.length() > 0) {
			userMentions = userMentions.substring(0, userMentions.length() - 1);
		}
		return userMentions;
	}

}
