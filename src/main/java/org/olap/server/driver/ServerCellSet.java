package org.olap.server.driver;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.olap.server.driver.metadata.ServerCell;
import org.olap.server.driver.util.MetadataUtils;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.CellSetAxisMetaData;
import org.olap4j.CellSetMetaData;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.Position;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Property;

public class ServerCellSet implements CellSet {

	private OlapStatement olapStatement;
	private boolean closed;
	private ArrayList<CellSetAxis> axes = new ArrayList<CellSetAxis>();
	private CellSetAxis filterAxis;
	private Cube cube;
	private Map< String, Cell  > cells = new HashMap< String, Cell>();
	
	public ServerCellSet(OlapStatement olapStatement, Cube cube) {
		this.olapStatement = olapStatement;
		this.cube = cube;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean next() throws SQLException {
		return false;
	}

	@Override
	public void close() throws SQLException {
		this.closed = true;
	}

	@Override
	public boolean wasNull() throws SQLException {
		return false;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public String getCursorName() throws SQLException {
		return null;
	}


	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return false;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		return false;
	}

	@Override
	public void beforeFirst() throws SQLException {
	}

	@Override
	public void afterLast() throws SQLException {
	}

	@Override
	public boolean first() throws SQLException {
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		return false;
	}

	@Override
	public int getRow() throws SQLException {
		return 0;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		return false;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		return false;
	}

	@Override
	public boolean previous() throws SQLException {
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		olapStatement.setFetchDirection(direction);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return olapStatement.getFetchDirection();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		olapStatement.setFetchSize(rows);
	}

	@Override
	public int getFetchSize() throws SQLException {
		return olapStatement.getFetchSize();
	}

	@Override
	public int getType() throws SQLException {
		return 0;
	}

	@Override
	public int getConcurrency() throws SQLException {
		return 0;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
	}

	@Override
	public void insertRow() throws SQLException {
	}

	@Override
	public void updateRow() throws SQLException {
	}

	@Override
	public void deleteRow() throws SQLException {
	}

	@Override
	public void refreshRow() throws SQLException {
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
	}

	@Override
	public void moveToInsertRow() throws SQLException {
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public int getHoldability() throws SQLException {
		return 0;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		throw new UnsupportedOperationException("Unsupported operation for OLAP cell set");
	}

	@Override
	public List<CellSetAxis> getAxes() {
		return axes;
	}

	@Override
	public CellSetAxis getFilterAxis() {
		return filterAxis;
	}

	@Override
	public Cell getCell(List<Integer> coordinates) {
		String skey = getKeyFromInts(coordinates);
		Cell cell = cells.get(skey);
		if(cell==null){
			cell = new ServerCell(this, coordinates, null, null);	
			cells.put(skey, cell);
		}
		return cell;
	}
	
	
	private String getKeyFromInts(List<Integer> coordinates){
		StringBuffer sb = new StringBuffer();
		for(Integer c : coordinates){
			sb.append(c).append('.');
		}
		return sb.toString();
	}

	@Override
	public Cell getCell(int ordinal) {
		return getCell( ordinalToCoordinates(ordinal) );
	}

	@Override
	public Cell getCell(Position... positions) {
		List<Integer> coordinates = new ArrayList<Integer>(axes.size());
		
		for(int i=0; i < axes.size(); i++){
			CellSetAxis axis = axes.get(i);
			int coord = axis.getPositions().indexOf(positions[i]);
			if(coord<0)
				return null;
			coordinates.set(i, coord);
		}
		
		return getCell(coordinates);
	}

	@Override
	public List<Integer> ordinalToCoordinates(int ordinal) {
		
		List<Integer> coordinates = new ArrayList<Integer>( axes.size() );
		
		
		List<Integer> factors = new ArrayList<Integer>( axes.size() );
		int factor = 1;
		for(int i=0; i< axes.size();i++){
			factors.set(i, factor);
			factor *= axes.get(i).getPositionCount();
		}
		
		int residual = ordinal;
		
		for(int i=axes.size()-1; i >= 0 && residual>=0 ; i--){
			
			int coord = residual / factors.get(i);						
			residual -= factors.get(i) * coord;		
			coordinates.set(i, coord);
		}
		

		return coordinates;
	}

	@Override
	public int coordinatesToOrdinal(List<Integer> coordinates) {		
		int ordinal = 0;		
		for(int i=axes.size()-1; i >= 0 && ordinal>=0 ; i--){			
			CellSetAxis axis = axes.get(i);
			ordinal += coordinates.get(i) + axis.getPositionCount() * ordinal; 
			
		}		
		return ordinal;
	}

	@Override
	public OlapStatement getStatement() throws SQLException {
		return olapStatement;
	}
	

	
	public Cell addCell(Integer[] integers, Number value, DecimalFormat format) {		
		List<Integer> positions = Arrays.asList(integers);
		ServerCell cell = new ServerCell(this, positions, value, format);
		String skey = getKeyFromInts(positions);
		cells.put(skey, cell);
		return cell;
		
	}

	
	@Override
	public CellSetMetaData getMetaData() throws OlapException {
		return new CellSetMetaData(){

			@Override
			public int getColumnCount() throws SQLException {				
				return axes.get(0).getPositionCount();
			}

			@Override
			public boolean isAutoIncrement(int column) throws SQLException {
				return false;
			}

			@Override
			public boolean isCaseSensitive(int column) throws SQLException {
				return false;
			}

			@Override
			public boolean isSearchable(int column) throws SQLException {
				return false;
			}

			@Override
			public boolean isCurrency(int column) throws SQLException {
				return false;
			}

			@Override
			public int isNullable(int column) throws SQLException {
				return columnNullable;
			}

			@Override
			public boolean isSigned(int column) throws SQLException {
				return false;
			}

			@Override
			public int getColumnDisplaySize(int column) throws SQLException {
				return 10;
			}

			@Override
			public String getColumnLabel(int column) throws SQLException {
				return axes.get(0).getPositions().get(column).toString();
			}

			@Override
			public String getColumnName(int column) throws SQLException {
				return getColumnLabel(column);
			}

			@Override
			public String getSchemaName(int column) throws SQLException {
				return getCube().getSchema().getName();
			}

			@Override
			public int getPrecision(int column) throws SQLException {
				return 0;
			}

			@Override
			public int getScale(int column) throws SQLException {
				return 0;
			}

			@Override
			public String getTableName(int column) throws SQLException {
				return getCube().getName();
			}

			@Override
			public String getCatalogName(int column) throws SQLException {
				return getCube().getSchema().getCatalog().getName();
			}

			@Override
			public int getColumnType(int column) throws SQLException {
				return 0;
			}

			@Override
			public String getColumnTypeName(int column) throws SQLException {
				return null;
			}

			@Override
			public boolean isReadOnly(int column) throws SQLException {
				return true;
			}

			@Override
			public boolean isWritable(int column) throws SQLException {
				return false;
			}

			@Override
			public boolean isDefinitelyWritable(int column) throws SQLException {
				return false;
			}

			@Override
			public String getColumnClassName(int column) throws SQLException {
				return null;
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				return null;
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
				return false;
			}

			@Override
			public NamedList<Property> getCellProperties() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Cube getCube() {
				return cube;
			}

			@Override
			public NamedList<CellSetAxisMetaData> getAxesMetaData() {
				return MetadataUtils.cellSetNamedList(axes);
			}
			

			@Override
			public CellSetAxisMetaData getFilterAxisMetaData() {
				return filterAxis==null ? null : filterAxis.getAxisMetaData();
			}
			
		};
	}

	public void setAxis(ServerCellSetAxis axis) {
		
		if(axis.getAxisOrdinal().isFilter()){
			this.filterAxis = axis;
		}else{
			axes.add(axis);
			Collections.sort(axes, new Comparator<CellSetAxis>() {

				@Override
				public int compare(CellSetAxis o1, CellSetAxis o2) {
					if(o1.getAxisOrdinal().axisOrdinal()>o2.getAxisOrdinal().axisOrdinal()){
						return 1;
					}else if(o1.getAxisOrdinal().axisOrdinal()<o2.getAxisOrdinal().axisOrdinal()){
						return -1;
					}else{
						return 0;
					}
				}
			});
		}
		
		
	}



	
}
