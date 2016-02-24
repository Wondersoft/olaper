package org.olap.server.database.physical;

import java.util.ArrayList;
import java.util.List;

import org.olap.server.database.CatalogDefinition;
import org.olap4j.OlapException;

import com.healthmarketscience.sqlbuilder.FunctionCall;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbJoin;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;

public class AggregateTable {
	
	public String table;
	public List<TableJoin> joins;
	public List<TableMeasure> measures;
	
	
	private DbTable dbTable;
	
	protected void index(DbSchema dbSchema, CatalogDefinition catalogDefinition) throws OlapException {
		dbTable = dbSchema.addTable(table);

		for(TableMeasure t : measures){
			if(t.formula==null){
				DbColumn dbColumn = dbTable.addColumn(t.column);
				t.setDbColumn(dbColumn);
				t.setExpression(FunctionCall.sum().addColumnParams(dbColumn));
			}
			
		}
		
		for(TableMeasure t : measures){
			if(t.formula!=null){
				Formula formula = new Formula(t.formula);
				t.setExpression(formula.render(this));	
			}				
		}
		
		
		List<DimensionTable> joinedDimensions = new ArrayList<DimensionTable>();
		for(TableJoin j : joins){
			
			 DbColumn fkey = dbTable.addColumn(j.key);
			 
			 DimensionTable dtable = catalogDefinition.getPhysicalSchema().findDimensionTable(j.table);
			 if(dtable==null)
				 throw new OlapException("Unknown table name used "+j.table+" in aggregate table "+ table);
			 
			 if(joinedDimensions.contains(dtable))
				 dtable = dtable.createAlias(dbSchema);
			 
			 joinedDimensions.add(dtable);
			 
			 DbJoin join = dbSchema.getSpec().createJoin(
					 dbTable, 
					 dtable.getDbTable(), 
					 new String[]{j.key}, 
					 new String[]{dtable.key});
			 
			    	    
			j.setDimensionTable(dtable); 
			j.setDbJoin(join);
			j.setForeign_key(fkey);
		}
		
	}

	public DbTable getDbTable() {
		return dbTable;
	}
}
