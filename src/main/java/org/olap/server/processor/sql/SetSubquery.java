package org.olap.server.processor.sql;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.olap.server.database.physical.DimensionTable;
import org.olap.server.database.physical.TableColumn;
import org.olap.server.database.physical.TableJoin;
import org.olap.server.processor.LevelMember;
import org.olap4j.OlapException;

import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.ComboCondition;
import com.healthmarketscience.sqlbuilder.Condition;
import com.healthmarketscience.sqlbuilder.CustomCondition;
import com.healthmarketscience.sqlbuilder.InCondition;
import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.Subquery;
import com.healthmarketscience.sqlbuilder.UnaryCondition;


public class SetSubquery {
	
	private TableJoin join;
	private TableColumn column;
	private Set<String> values;
	private String from_value, to_value;
	private String having_expression;
	
	
	public SetSubquery(TableJoin join, TableColumn column, List<String> values) {
		this.join = join;
		this.column = column;
		this.values = new HashSet<String>(values);
	}

	
	public SetSubquery(TableJoin join, TableColumn column, String from_value, String to_value) {
		this.join = join;
		this.column = column;
		this.from_value = from_value;
		this.to_value = to_value;
		
	}


	public SetSubquery(String havingExpressionString) {
		this.having_expression = havingExpressionString;
	}


	public void or(SetSubquery q) throws OlapException {
		
		if( q.column!=column )
			throw new OlapException("Incompatible members in set "+q.column.column+" vs "+column.column);
		
		if(values!=null && q.values!=null)
			values.addAll(q.values);
		
		if(q.from_value!=null)
			this.from_value = q.from_value;
		
		if(q.to_value!=null)
			this.to_value = q.to_value;
	}

	

	public Condition condition() {
		
		if((values==null || values.size()==0) && from_value==null && to_value==null)
			return null;
			
		DimensionTable dim_table = join.getDimensionTable();
		
		SelectQuery dim_query = new SelectQuery().
				addFromTable(dim_table.getDbTable()).
				addColumns(dim_table.getDbKey());
		
		if(from_value!=null){
			dim_query = dim_query.addCondition(BinaryCondition.greaterThan(column.getDbColumn(), from_value, true));
		}

		if(to_value!=null){
			dim_query = dim_query.addCondition(BinaryCondition.lessThan(column.getDbColumn(), to_value, true));
		}

		
		if(values!=null && values.size()>0){
			if(values.size()==1){
				String value = values.iterator().next();
				if(LevelMember.NULL_MEMBER.equals(value))
					dim_query = dim_query.addCondition(UnaryCondition.isNull(column.getDbColumn()));
				else		
					dim_query = dim_query.addCondition(BinaryCondition.equalTo(column.getDbColumn(), value));
			}else{ 
				
				if(values.contains(LevelMember.NULL_MEMBER)){
					Set<String> values_without_null = new HashSet<String>(values);
					values_without_null.remove(LevelMember.NULL_MEMBER);
					dim_query = dim_query.addCondition(ComboCondition.or(
							UnaryCondition.isNull(column.getDbColumn()),
							new InCondition(column.getDbColumn(), values_without_null)));
				}else{
					dim_query = dim_query.addCondition(new InCondition(column.getDbColumn(), values));	
				}
				
			}
		}
		
		return new InCondition(join.getForeign_key(), new Subquery(dim_query) );
		
	}
	
	public Condition havingCondition(){
		if(having_expression==null)
			return null;
		
		return new CustomCondition(having_expression);
	}

	public void setHaving_expression(String having_expression) {
		this.having_expression = having_expression;
	}


}
