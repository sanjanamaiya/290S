package com.systems.s290.data;

import java.sql.SQLException;

public interface DataSplit {
	
	public void split(SystemDetails mDetails) throws SQLException;
	public void recreate(SystemDetails mDetails) throws SQLException;

}
