package org.olap.server.database.physical;

import java.util.ArrayList;
import java.util.List;

import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;

public class DimensionTable {
	
	public String table, key;
	
	public List<TableColumn> columns;
	
	private DbTable dbTable;
	private DbColumn dbKey;
	
	protected void index(DbSchema dbSchema) {
		dbTable = dbSchema.addTable(table);
		dbKey = dbTable.addColumn(key);
		
		for(TableColumn t : columns){
			t.setDbColumn(dbTable.addColumn(t.column));
		}
		
	}

	public DbTable getDbTable() {
		return dbTable;
	}

	public DbColumn getDbKey() {
		return dbKey;
	}
	
	public TableColumn findColumnByAttribute(String attribute_name){
		for(TableColumn c : columns){
			if(c.attribute!=null && attribute_name.equals(c.attribute))
				return c;
		}
		return null;
	}

	
	public TableColumn findColumnByName(String name){
		for(TableColumn c : columns){
			if( name.equals(c.column))
				return c;
		}
		return null;
	}

	public DimensionTable createAlias(DbSchema dbSchema) {
		
		DimensionTable alias = new DimensionTable();
		
		alias.key = key;
		alias.table = table;
		
		alias.columns = new ArrayList<TableColumn>();
		
		for(TableColumn tc : columns){
			TableColumn alias_tc = new TableColumn();
			
			alias_tc.attribute = tc.attribute;
			alias_tc.column = tc.column;
			alias_tc.identified_by = tc.identified_by;
			alias_tc.sort_direction = tc.sort_direction;
			alias_tc.sorted_by = tc.sorted_by;
			
			alias.columns.add(alias_tc);
		}
		
		alias.index(dbSchema);
		
		return alias;
	}
	
	
}
