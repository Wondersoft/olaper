package org.olap.server.processor.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.olap.server.driver.metadata.LiteralMeasure;
import org.olap.server.driver.metadata.ServerCube;
import org.olap.server.driver.metadata.ServerMeasure;
import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.LiteralNode;
import org.olap4j.metadata.Cube;

public class LiteralMember implements OlapOp {

	private LiteralNode node;
	private ServerCube cube;
	
	public LiteralMember(LiteralNode node, Cube cube) {
		this.node = node;
		this.cube = (ServerCube) cube;
	}

	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {		
		ServerMeasure literal_measure = new LiteralMeasure( cube.getMeasureDimension(), node);
		return Collections.singletonList(new LevelMemberSet(literal_measure, node, this));
	}

	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer) throws OlapException {
		return null;
	}

	@Override
	public Collection<ServerMeasure> getAllMeasuresUsed() throws OlapException {
		return Collections.emptyList();
	}

}
