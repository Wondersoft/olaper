package org.olap.server.processor.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.olap4j.OlapException;

public interface ResultSetProcessor {

	public void process(ResultSet rs) throws SQLException, OlapException;
	
}
