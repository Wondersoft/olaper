package org.olap.server.database.physical;

import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;

public class TableColumn {
	public String column, attribute, identified_by, sorted_by;
	public String sort_direction = "ASC";
	
	private DbColumn dbColumn;

	public DbColumn getDbColumn() {
		return dbColumn;
	}

	public void setDbColumn(DbColumn dbColumn) {
		this.dbColumn = dbColumn;
	}
}
