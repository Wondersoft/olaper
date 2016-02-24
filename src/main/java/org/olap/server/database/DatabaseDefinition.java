package org.olap.server.database;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.olap.server.driver.util.ResourceLoader;

public class DatabaseDefinition  extends NamedElement {
	
	public List<CatalogDefinition> catalogs;

	private static Map<String, DatabaseDefinition> databases = new HashMap<String, DatabaseDefinition>();
	
	public static synchronized DatabaseDefinition getDatabase(String url) throws Exception{
		DatabaseDefinition database = databases.get(url);
		if(database==null){
			database=ResourceLoader.read(url, DatabaseDefinition.class);
		}
		return database;
	}	
}
