package org.olap.server.driver.metadata;

import org.olap.server.database.CatalogDefinition;
import org.olap.server.database.physical.PhysicalSchema;
import org.olap.server.driver.util.MetadataUtils;
import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Database;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Schema;

public class ServerCatalog implements Catalog {

	private Database database;
	private CatalogDefinition definition;
	
	public ServerCatalog(Database database, CatalogDefinition definition) {
		this.database = database;
		this.definition = definition;
	}

	@Override
	public NamedList<Schema> getSchemas() throws OlapException {
		return MetadataUtils.singletoneNamedList(new ServerSchema(this, definition));
	}

	@Override
	public String getName() {
		return definition.name;
	}

	@Override
	public OlapDatabaseMetaData getMetaData() {
		throw new UnsupportedOperationException("getMetaData");
	}

	@Override
	public Database getDatabase() {
		return database;
	}

	public CatalogDefinition getDefinition() {
		return definition;
	}

	public PhysicalSchema getPhysicalSchema() throws OlapException {
		return definition.getPhysicalSchema();
	}

}
