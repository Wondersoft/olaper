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
			
		
	/*	
		// Columns
		Measure m = cube.getMeasures().get(0);
		
		List<Position> measures = new ArrayList<Position>();
		measures.add(new ServerPosition((List)Collections.singletonList(m), 0));
		
		List<Hierarchy> meas_h = new ArrayList<Hierarchy>();
		meas_h.add(m.getHierarchy());
		
		cellSet.addAxis(0, measures, new ArrayList<Hierarchy>());
		
		
		// Rows 
		List<Position> rows = new ArrayList<Position>();
		Dimension dim = cube.getDimensions().get(0);
		Hierarchy h = dim.getHierarchies().get(0);
		Level l = h.getLevels().get(0);
		

		rows.add(new ServerPosition((List)Collections.singletonList( new ServerLevelMember(l, "BLOCK1", 0)), 0));
		rows.add(new ServerPosition((List)Collections.singletonList( new ServerLevelMember(l, "BLOCK2", 0)), 1));
		rows.add(new ServerPosition((List)Collections.singletonList( new ServerLevelMember(l, "BLOCK3", 0)), 2));
		
		List<Hierarchy> dim_h = new ArrayList<Hierarchy>();
		dim_h.add(h);
		
		
		
		cellSet.addAxis(1, rows, dim_h);
		
		cellSet.setFilterAxis(new ArrayList<Position>(), new ArrayList<Hierarchy>());
		
		
		cellSet.addCell( new Integer[]{0,0}, 120);
		cellSet.addCell( new Integer[]{0,1}, 121);
		cellSet.addCell( new Integer[]{0,2}, 122);
		
		
		*/
		
		return cellSet;
	}



	protected void fetchAxisMetaData(ServerCellSet cellSet) throws OlapException {
		
		for(AxisNode axisNode : selectNode.getAxisList()){
			cellSet.setAxis( new ResultAxis(cellSet, axisNode ) );
		}

		cellSet.setAxis( new ResultAxis(cellSet, selectNode.getFilterAxis()) );

	}


}
