package com.systems.s290.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.systems.s290.data.ConsistentHashStrategy;
import com.systems.s290.data.HashingStrategy;
import com.systems.s290.data.SplitTemplate;
import com.systems.s290.data.StaticHashStrategy;
import com.systems.s290.data.SystemDetails;
import com.systems.s290.db.connection.MySQLDataSource;

public class RequestHandler 
{
	static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);
	SystemDetails sysDetails = null;
	
	public RequestHandler()
	{
		List<String> targetConnectionStrings = new ArrayList<String>();
		try(BufferedReader reader = new BufferedReader(new FileReader(new File("resources" + File.pathSeparator + "serverconfig.txt"))))
		{
			if (reader != null)
			{	
				String serverName = null;
				while((serverName = reader.readLine()) != null)
				{
					targetConnectionStrings.add(serverName);
				}
			}
		}
		catch (IOException e)
		{
			LOG.error("Unable to read serverconfig file, cannot load server details", e );
		}
		
		sysDetails = new SystemDetails();
		sysDetails.setTargetConnectionStrings(targetConnectionStrings);
	}
	public static String CONSISTENT = "consistent";
	public static String STATIC = "static";
	public void getTweetsFromUser(String userId, String hashType)
	{
		long user = Long.parseLong(userId);
		ConsistentHashStrategy consistentStrategy = null; 
		StaticHashStrategy staticStrategy = null; 
		
		// todo : from interface
		if (hashType.equals(CONSISTENT))
		{
			consistentStrategy = new ConsistentHashStrategy(sysDetails);
			int bucket = consistentStrategy.getHash(user);
			requestUserInformation(sysDetails.getTargetConnectionStrings().get(bucket), consistentStrategy.getTargetTableName(), user);
		}
		else
		{
			staticStrategy = new StaticHashStrategy(sysDetails);
			int bucket = staticStrategy.getHash(user, sysDetails.getServerCount());
			requestUserInformation(sysDetails.getTargetConnectionStrings().get(bucket), staticStrategy.getTargetTableName(), user);
		}
	}
	
	private void requestUserInformation(String serverConnString, String tableName, long userId) 
	{
		String reqUSerString = "select * from " + tableName + " where UserId = " + userId;
		try(Connection conn = MySQLDataSource.getConnection(serverConnString))
		{
			Statement stmt = conn.createStatement();
			stmt.execute(reqUSerString);
		}
		catch(SQLException e)
		{
			// log, and continue
			LOG.warn("error retrieving user info from table " + tableName, e);
		}
		
	}
}
