package org.olap.server.processor.functions;

import java.util.ArrayList;
import java.util.List;

import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Cube;

public abstract class AbstractArgFunction extends OlapFunction {

	protected List<OlapOp> arguments = new ArrayList<OlapOp>();
	
	public AbstractArgFunction(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
		for(ParseTreeNode arg: node.getArgList() ) {
			arguments.add(OlapFunctionFactory.INSTANCE.function( arg, cube));
		}
	}


}
