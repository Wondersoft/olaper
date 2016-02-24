package org.olap.server.database.physical;

import com.healthmarketscience.sqlbuilder.Expression;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;

public class TableMeasure {

	public String measure, column, formula;
	private DbColumn dbColumn;
	
	private Expression expression;

	public Expression getExpression() {
		return expression;
	}

	public void setExpression(Expression expression) {
		this.expression = expression;
	}

	public DbColumn getDbColumn() {
		return dbColumn;
	}

	public void setDbColumn(DbColumn dbColumn) {
		this.dbColumn = dbColumn;
	}
	
}
