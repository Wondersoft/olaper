package org.olap.server.driver;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;



public class OlapServerDriver implements Driver {

    static {
        try {
            register();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    
    private static void register() throws SQLException {
        DriverManager.registerDriver(new OlapServerDriver());
    }

	
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
        if (ServerConnection.acceptsURL(url)) {
        	return new ServerConnection(url, info);
        }else{
        	return null;
        }
 	}


	@Override
	public boolean acceptsURL(String url) throws SQLException {		
		return ServerConnection.acceptsURL(url);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
        List<DriverPropertyInfo> list = new ArrayList<DriverPropertyInfo>();
        for (Map.Entry<Object, Object> entry : info.entrySet()) {
            list.add(
                new DriverPropertyInfo(
                    (String) entry.getKey(),
                    (String) entry.getValue()));
        }
        return list.toArray(new DriverPropertyInfo[list.size()]);

	}

	@Override
	public int getMajorVersion() {
		return 1;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return Logger.getLogger(OlapServerDriver.class.getName());
	}

}
