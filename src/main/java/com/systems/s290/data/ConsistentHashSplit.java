package com.systems.s290.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.consistenthash.ConsistentHash;
import com.systems.s290.db.connection.MySQLDataSource;

public class ConsistentHashSplit implements DataSplit {

	static final Logger LOG = LoggerFactory.getLogger(DataSplit.class);

	public void split(SystemDetails systemDetails) throws SQLException {

		ConsistentHash<String> consistentHash = new ConsistentHash<String>(5, systemDetails.getConnectionStrings());

		// read from source, hash and populate to target
		Connection source = MySQLDataSource.getConnection(systemDetails.getSourceConnectionString());
		ResultSet rs = source.createStatement().executeQuery(systemDetails.getSelectQuery());
		while (rs.next()) {
			// bin for this key
			String primaryKeyValue = rs.getString(systemDetails.getKey());
			String bin = consistentHash.getBinFor(primaryKeyValue);
			Connection target = MySQLDataSource.getConnection(bin);

			try {
				PreparedStatement preparedStatement = target.prepareStatement(systemDetails.getInsertQuery());
				for (int i = 0; i < systemDetails.getColumnCount(); i++) {
					preparedStatement.setString(i+1, rs.getString(i + 1));
				}

				preparedStatement.execute();
			} catch (Exception e) {
				LOG.info("Exception while writing "+primaryKeyValue, e);
				
			} finally {
				target.close();
			}

		}

		source.close();

	}

}
