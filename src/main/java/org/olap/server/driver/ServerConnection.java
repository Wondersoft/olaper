package org.olap.server.driver;

import java.io.StringWriter;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.olap.server.driver.metadata.ServerDatabase;
import org.olap4j.OlapConnection;
import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.PreparedOlapStatement;
import org.olap4j.Scenario;
import org.olap4j.mdx.ParseTreeWriter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;
import org.olap4j.mdx.parser.impl.DefaultMdxParserImpl;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Database;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Schema;

public class ServerConnection implements OlapConnection{

	private static final String URL_PREFIX = "jdbc:olapserver:";
	private String databaseName;
	private String catalogName;
	private String schemaName;
	private String roleName;
	private Locale locale;
	
	private Scenario scenario;
	private Database database;
	
	private String url;
	private Properties info;
	
	private boolean closed = false;
	
	public ServerConnection(String url, Properties info) {
		this.url = url;
		this.info = info;
	}

	public static boolean acceptsURL(String url) {
		return url!=null && url.startsWith(URL_PREFIX);
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public String nativeSQL(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public boolean getAutoCommit() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public void commit() throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void rollback() throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void close() throws SQLException {
		this.closed = true;
	}
	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}
	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int getTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setHoldability(int holdability) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public Savepoint setSavepoint() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Clob createClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Blob createBlob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public NClob createNClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public SQLXML createSQLXML() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public boolean isValid(int timeout) throws SQLException {
		return !closed;
	}
	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public String getClientInfo(String name) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Properties getClientInfo() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void abort(Executor executor) throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface) ) {
            return iface.cast(this);
        }
        throw new SQLException("does not implement '" + iface + "'");
	}
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}
	@Override
	public OlapDatabaseMetaData getMetaData() throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public PreparedOlapStatement prepareOlapStatement(String mdx)
			throws OlapException {
		return new ServerPreparedOlapStatement(this, mdx);
	}
	@Override
	public MdxParserFactory getParserFactory() {
        return new MdxParserFactory() {
            public MdxParser createMdxParser(OlapConnection connection) {
                return new DefaultMdxParserImpl();
            }

            public MdxValidator createMdxValidator(OlapConnection connection) {
                return new MdxValidator(){

					@Override
					public SelectNode validateSelect(SelectNode selectNode)
							throws OlapException {
						StringWriter sw = new StringWriter();
			            selectNode.unparse(new ParseTreeWriter(sw));
			            return selectNode;
					}
                	
					
                };
            }
        };

	}
	@Override
	public OlapStatement createStatement() throws OlapException {
		return new ServerOlapStatement(this);
	}
	@Override
	public String getDatabase() throws OlapException {
		return databaseName;
	}
	@Override
	public void setDatabase(String databaseName) throws OlapException {
		this.databaseName = databaseName;
		
	}
	@Override
	public Database getOlapDatabase() throws OlapException {
		if(database==null)
			database = new ServerDatabase(this, url.replace(URL_PREFIX, ""), info);
		return database;
	}
	@Override
	public NamedList<Database> getOlapDatabases() throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public String getCatalog() throws OlapException {
		return catalogName;
	}
	@Override
	public void setCatalog(String catalogName) throws OlapException {
		this.catalogName = catalogName;
	}
	@Override
	public Catalog getOlapCatalog() throws OlapException {
		return getOlapCatalogs().get(catalogName);
	}
	@Override
	public NamedList<Catalog> getOlapCatalogs() throws OlapException {
		return getOlapDatabase().getCatalogs();
	}
	@Override
	public String getSchema() throws OlapException {
		return schemaName;
	}
	@Override
	public void setSchema(String schemaName) throws OlapException {
		this.schemaName = schemaName;
	}
	@Override
	public Schema getOlapSchema() throws OlapException {
		return getOlapSchemas().get(catalogName);
	}
	@Override
	public NamedList<Schema> getOlapSchemas() throws OlapException {
		return getOlapCatalog().getSchemas();
	}
	@Override
	public void setLocale(Locale locale) {
		this.locale = locale;
	}
	@Override
	public Locale getLocale() {
		return locale;
	}
	@Override
	public void setRoleName(String roleName) throws OlapException {
		this.roleName = roleName;
	}
	@Override
	public String getRoleName() {
		return roleName;
	}
	@Override
	public List<String> getAvailableRoleNames() throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Scenario createScenario() throws OlapException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setScenario(Scenario scenario) throws OlapException {
		this.scenario = scenario;
	}
	@Override
	public Scenario getScenario() throws OlapException {
		return scenario;
	}

}
