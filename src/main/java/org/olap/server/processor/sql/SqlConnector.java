package org.olap.server.processor.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap.server.driver.metadata.ServerCatalog;
import org.olap4j.OlapException;

public class SqlConnector {

	
	private static Map<String, SqlConnector> map = new HashMap<String, SqlConnector>();
	
	private DataSource dataSource;
	
	private static Log log = LogFactory.getLog(SqlConnector.class);
	
	private SqlConnector(String ds) {
		dataSource = setupDataSource(ds);
	}
	
	public Connection getConnection() throws OlapException{
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			throw new OlapException("Fail to get connection", e);
		}		
	}

	public static synchronized SqlConnector getConnector(ServerCatalog catalog) throws OlapException{
		
		String ds = catalog.getPhysicalSchema().dataSource;
		
		if(map.containsKey(ds))
			return map.get(ds);
		
		SqlConnector con = new SqlConnector(ds);
		map.put(ds, con );
		
		return con;
		
		
	}
	
	public void execute(String query, ResultSetProcessor processor) throws OlapException {
		
		log.info(query);
		
		long start = System.currentTimeMillis();
		
		Connection con = getConnection();
		log.debug("Query took "+(System.currentTimeMillis()-start)+"ms to connect");	
		start = System.currentTimeMillis();
		
		Statement stmt = null;
		ResultSet rset = null;
		
		try{
			
			stmt = con.createStatement();
			rset = stmt.executeQuery(query);
			
			log.info("Query took "+(System.currentTimeMillis()-start)+"ms to execute");			
			start = System.currentTimeMillis();
			
			processor.process(rset);
			
			log.debug("Query took "+(System.currentTimeMillis()-start)+"ms to process");
			
		}catch(SQLException ex){
			throw new OlapException("Error "+ ex.getMessage() +" executing query "+query, ex);
		}finally{
	          try { if (rset != null) rset.close(); } catch(Exception e) {
	        	  log.warn("Fail close result set", e);
	          }
	          try { if (stmt != null) stmt.close(); } catch(Exception e) {
	        	  log.warn("Fail close statement", e);
	          }
	          try { if (con != null) con.close(); } catch(Exception e) { 
	        	  log.warn("Fail close connection", e);
	          }
		}
	}

	
	   public DataSource setupDataSource(String connectURI) {
	        BasicDataSource dataSource = new BasicDataSource();	        
	        dataSource.setUrl(connectURI);
	        return dataSource;
	    }
	
}
