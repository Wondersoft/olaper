package org.olap.server.database;

import java.util.List;

import org.olap.server.database.physical.PhysicalSchema;
import org.olap.server.driver.util.ResourceLoader;
import org.olap4j.OlapException;


public class CatalogDefinition  extends NamedElement{
	public List<SharedDimensionDefinition> dimensions;
	public List<CubeDefinition> cubes;
	public String physical_schema;
	
	private PhysicalSchema schema;
	
	public PhysicalSchema getPhysicalSchema() throws OlapException {
		if(schema==null){			
			try{
				schema = ResourceLoader.read(physical_schema, PhysicalSchema.class);		
				schema.index(this);
			}catch(Exception ex){
				throw new OlapException("Fail to read physical schema: "+ex.getMessage(), ex);
			}
		}
		return schema;
		
	}
	
	
	
	
	
}
