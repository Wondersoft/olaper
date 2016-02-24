package org.olap.server.processor.functions;

import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "filter")
public class Filter extends AbstractSetFunction {

	public Filter(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
	}

	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer) throws OlapException{	
		
		
		CallNode conditionalNode = (CallNode)node.getArgList().get(1);

		SetSubquery result =  argument.query(mapping, layer);
		if(result==null)
			result = new SetSubquery(buildExpressionString(conditionalNode, mapping));
		else
			result.setHaving_expression(buildExpressionString(conditionalNode, mapping));
		
		return result;
		
		
	}

}
