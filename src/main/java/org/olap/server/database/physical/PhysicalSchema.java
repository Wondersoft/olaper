package org.olap.server.database.physical;

import java.util.List;

import org.olap.server.database.CatalogDefinition;
import org.olap4j.OlapException;
import org.olap4j.metadata.Dimension;

import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;

public class PhysicalSchema {
	
	public String schema;
	public String dataSource;
	
	public List<DimensionTable> dimensions;
	public List<AggregateTable> aggregates;
	
	private DbSpec dbSpec;
	private DbSchema dbSchema;

	public void index(CatalogDefinition catalogDefinition) throws OlapException {
		
		dbSpec = new DbSpec();
		dbSchema = dbSpec.addSchema(schema);
		
		for(DimensionTable t : dimensions){
			t.index(dbSchema);
		}
		
		for(AggregateTable t : aggregates){
			t.index(dbSchema, catalogDefinition);
		}
		
		
	}

	public DbSpec getDbSpec() {
		return dbSpec;
	}

	public DbSchema getDbSchema() {
		return dbSchema;
	}
	
	public DimensionTable findDimensionTable(String name){
		for(DimensionTable dt : dimensions){
			if(dt.table.equals(name))
				return dt;
		}
		return null;
	}
	
	public DimensionTable findDimensionTable(Dimension dimension){
		
		for(AggregateTable at : aggregates){
			for(TableJoin join: at.joins){
				if(join.dimension.equals(dimension.getName()))
					return findDimensionTable(join.table);
			}
		}
		
		return null;
	}

}
