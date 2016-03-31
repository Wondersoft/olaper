package org.olap.server.processor.functions;

import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "except")
public class Except extends AbstractSetFunction {

	private OlapOp exceptNode;
	public Except(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
		this.exceptNode = OlapFunctionFactory.INSTANCE.function( node.getArgList().get(1), cube);
	}

	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer) throws OlapException{			
		SetSubquery baseQuery =  argument.query(mapping, layer);
		return baseQuery.except(exceptNode.query(mapping, layer));
	}

}
