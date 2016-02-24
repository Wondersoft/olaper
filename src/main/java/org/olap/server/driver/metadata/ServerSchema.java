package org.olap.server.driver.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.olap.server.database.CatalogDefinition;
import org.olap.server.database.CubeDefinition;
import org.olap.server.database.SharedDimensionDefinition;
import org.olap.server.driver.util.MetadataUtils;
import org.olap4j.OlapException;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Schema;

public class ServerSchema implements Schema {

	private CatalogDefinition definition;
	private ServerCatalog serverCatalog;
	
	public ServerSchema(ServerCatalog serverCatalog,
			CatalogDefinition definition) {
		this.definition = definition;
		this.serverCatalog = serverCatalog;
	}

	@Override
	public Catalog getCatalog() {
		return serverCatalog;
	}

	@Override
	public String getName() {
		return definition.name;
	}

	@Override
	public NamedList<Cube> getCubes() throws OlapException {
		
		NamedList<Cube> list = MetadataUtils.metadataNamedList(new ArrayList<Cube>());
		
	   for(CubeDefinition def : definition.cubes){
		   list.add(new ServerCube(this, def));  
	   }
	
	   return list;
	}

	@Override
	public NamedList<Dimension> getSharedDimensions() throws OlapException {
		NamedList<Dimension> list = MetadataUtils.metadataNamedList(new ArrayList<Dimension>());	            
	   for(SharedDimensionDefinition def : definition.dimensions){
		   list.add(new ServerDimension(def));  
	   }	
	   return list;
	}

	@Override
	public Collection<Locale> getSupportedLocales() throws OlapException {
		return Collections.emptyList();
	}

}
