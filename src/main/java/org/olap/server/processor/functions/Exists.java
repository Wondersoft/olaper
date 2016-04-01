package org.olap.server.processor.functions;

import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "exists")
public class Exists extends AbstractSetFunction {

	private OlapOp existsNode;
	public Exists(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
		this.existsNode = OlapFunctionFactory.INSTANCE.function( node.getArgList().get(1), cube);		
	}
	
	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer) throws OlapException{			
		SetSubquery baseQuery =  argument.query(mapping, layer);
		return baseQuery.exists(existsNode.query(mapping, layer));
	}
}
