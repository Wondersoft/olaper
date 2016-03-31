package org.olap.server.processor;

import org.olap.server.driver.ServerCellSet;
import org.olap.server.driver.ServerConnection;
import org.olap.server.driver.ServerOlapStatement;
import org.olap.server.driver.metadata.ServerCatalog;
import org.olap.server.driver.metadata.ServerSchema;
import org.olap.server.driver.util.ParseUtils;
import org.olap.server.processor.sql.SqlConnector;
import org.olap.server.processor.sql.SqlQuery;
import org.olap4j.CellSet;
import org.olap4j.OlapException;
import org.olap4j.mdx.AxisNode;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.SelectNode;
import org.olap4j.metadata.Cube;

public class Query {

	private ServerOlapStatement serverOlapStatement;
	private ServerConnection serverConnection;
	private SelectNode selectNode;
	private ServerSchema schema;
	private ServerCatalog catalog;
	private Cube cube;

	
	public Query(ServerOlapStatement serverOlapStatement, SelectNode node) throws OlapException {
		
		this.serverOlapStatement = serverOlapStatement;
		
		this.serverConnection = serverOlapStatement.getServerConnection();
		this.schema = (ServerSchema) serverConnection.getOlapSchema();
		this.catalog = (ServerCatalog) serverConnection.getOlapCatalog();
		
		this.selectNode = ParseUtils.normalize(serverOlapStatement.getMdxParser(), node);
	
		String cubeName = ParseUtils.identifierNames((IdentifierNode)this.selectNode.getFrom())[0];
		this.cube = schema.getCubes().get(cubeName);
		
		if(this.cube==null)
			throw new OlapException("Cube "+cubeName+" not found in schema "+schema.getName());
		

	}

	public CellSet execute() throws OlapException {
		
		ServerCellSet cellSet = new ServerCellSet(serverOlapStatement, cube); 
				
		fetchAxisMetaData(cellSet);
		
		SqlQuery query = new SqlQuery(catalog.getPhysicalSchema(), cellSet);
		
		query.generateQuery();

		SqlConnector exec = SqlConnector.getConnector(catalog);
		
		exec.execute(query.toString(), query);
		
		
		return cellSet;
	}



	protected void fetchAxisMetaData(ServerCellSet cellSet) throws OlapException {
		
		for(AxisNode axisNode : selectNode.getAxisList()){
			cellSet.setAxis( new ResultAxis(cellSet, axisNode ) );
		}

		cellSet.setAxis( new ResultAxis(cellSet, selectNode.getFilterAxis()) );

	}


}
