package org.olap.server.processor.sql;

import com.healthmarketscience.sqlbuilder.OrderObject.Dir;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;

public class Sorter {
	
	private Dir direction;
	private DbColumn column;
	private String expression;
	private boolean breakingHierarchy;
	
		
	public Sorter(DbColumn dbColumn, String direction) {
		this.column = dbColumn;
		this.direction = parseDir(direction);

	}

	public Sorter(String expression, String direction) {
		this.expression = expression;
		this.direction = parseDir(direction);
		this.breakingHierarchy = parseBreakingHierarchy(direction);		

	}
	
	private static boolean parseBreakingHierarchy(String direction){
		return direction.toLowerCase().startsWith("b");
	}
	
	private static Dir parseDir(String direction){
		return direction.toLowerCase().startsWith("asc") ? Dir.ASCENDING : Dir.DESCENDING;
	}

	
	public Dir getDirection() {
		return direction;
	}
	public DbColumn getColumn() {
		return column;
	}
	public boolean isBreakingHierarchy() {
		return breakingHierarchy;
	}

	public void setBreakingHierarchy(boolean breakingHierarchy) {
		this.breakingHierarchy = breakingHierarchy;
	}

	public boolean isBelongs_to_hierarchy() {
		return this.column != null;
	}

	public String getExpression() {
		return expression;
	}
	
	public void setDirectionString(String str){
		this.direction = parseDir(str);
	}
}
