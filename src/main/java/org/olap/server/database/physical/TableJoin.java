package org.olap.server.database.physical;

import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbJoin;

public class TableJoin {

	public String dimension, table, key;

	private DbJoin dbJoin;
	private DimensionTable dimensionTable;
	private DbColumn foreign_key;
	
	public DbJoin getDbJoin() {
		return dbJoin;
	}

	public void setDbJoin(DbJoin dbJoin) {
		this.dbJoin = dbJoin;
	}

	public DimensionTable getDimensionTable() {
		return dimensionTable;
	}

	public void setDimensionTable(DimensionTable dimensionTable) {
		this.dimensionTable = dimensionTable;
	}

	public DbColumn getForeign_key() {
		return foreign_key;
	}

	public void setForeign_key(DbColumn foreign_key) {
		this.foreign_key = foreign_key;
	}
	
}
