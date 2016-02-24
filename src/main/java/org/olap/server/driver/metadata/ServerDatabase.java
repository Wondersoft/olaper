package org.olap.server.driver.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.olap.server.database.CatalogDefinition;
import org.olap.server.database.DatabaseDefinition;
import org.olap.server.driver.util.MetadataUtils;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Database;
import org.olap4j.metadata.NamedList;

public class ServerDatabase implements Database{

	private OlapConnection serverConnection;
	private DatabaseDefinition definition; 
	private String url;
	
	public ServerDatabase(OlapConnection serverConnection, String url,
			Properties info) throws OlapException {
			
		this.serverConnection = serverConnection;
		this.url = url;
		try{
			this.definition = DatabaseDefinition.getDatabase(url);
		}catch(Exception ex){
			throw new OlapException("Failed to connect to "+url+", unable to read datasource due to "+ex.getMessage(), ex);
		}
			
	}

	@Override
	public OlapConnection getOlapConnection() {
		return serverConnection;
	}

	@Override
	public String getName() throws OlapException {
		return definition.name;
	}

	@Override
	public String getDescription() throws OlapException {
		return definition.caption;
	}

	@Override
	public String getURL() throws OlapException {
		return url;
	}

	@Override
	public String getDataSourceInfo() throws OlapException {
		return definition.name;
	}

	@Override
	public String getProviderName() throws OlapException {
		return getClass().getSimpleName();
	}

	@Override
	public List<ProviderType> getProviderTypes() throws OlapException {
		return Collections.singletonList(ProviderType.MDP);
	}

	@Override
	public List<AuthenticationMode> getAuthenticationModes()
			throws OlapException {
		return Collections.singletonList(AuthenticationMode.Unauthenticated);
	}

	@Override
	public NamedList<Catalog> getCatalogs() throws OlapException {
		
		NamedList<Catalog> list = MetadataUtils.catalogNamedList(new ArrayList<Catalog>());
	    for(CatalogDefinition cdef: definition.catalogs){
	    	list.add(new ServerCatalog(this, cdef));
	    }

		return list;
	}

}
