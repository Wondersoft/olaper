package org.olap.server.processor;

import java.util.List;

import org.olap.server.driver.metadata.ServerCatalog;
import org.olap.server.driver.metadata.ServerLevel;
import org.olap.server.driver.metadata.ServerMeasureDimension;
import org.olap.server.processor.sql.SqlConnector;
import org.olap.server.processor.sql.SqlMembersQuery;
import org.olap4j.OlapException;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;

public class MemberQuery {


	private Level level;
	private ServerCatalog catalog;
	
	public MemberQuery(Catalog catalog, Level level) throws OlapException {

		this.level = level;
		this.catalog = (ServerCatalog) catalog;
		
	}

	public List<Member> getMembers() throws OlapException {
		
		if(! (level instanceof ServerLevel ) )
			return level.getMembers();
		
		SqlMembersQuery query = new SqlMembersQuery(catalog.getPhysicalSchema(), level);

		query.generateQuery();

		SqlConnector exec = SqlConnector.getConnector(catalog);
		
		exec.execute(query.toString(), query);
		
		return query.getMembers();
	}

}
