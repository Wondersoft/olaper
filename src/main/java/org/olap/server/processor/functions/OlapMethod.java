package org.olap.server.processor.functions;

import java.util.Collection;
import java.util.Collections;

import org.olap.server.driver.metadata.ServerMeasure;
import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.metadata.Cube;

public abstract class OlapMethod implements OlapOp{

	protected IdentifierNode node;
	protected Cube cube;

	public OlapMethod(IdentifierNode node, Cube cube){
		this.node = node;
		this.cube = cube;
	}
	
	
	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer) throws OlapException {
		return null;
	}
	
	@Override
	public Collection<ServerMeasure> getAllMeasuresUsed(){
		return Collections.emptyList();
	}


}
