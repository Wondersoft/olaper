package org.olap.server.processor.functions;


import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.Sorter;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "order")
public class Order extends AbstractSetFunction {

	public Order(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
	}


	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer) throws OlapException{	
		
		
		ParseTreeNode sortBy = node.getArgList().get(1);
		ParseTreeNode sortDir = node.getArgList().get(2);

		String sortExpr = buildExpressionString(sortBy, mapping);
		
		layer.setSorter(new Sorter(sortExpr, sortDir.toString()));

		return argument.query(mapping, layer);
		
		
	}

	
	
}
