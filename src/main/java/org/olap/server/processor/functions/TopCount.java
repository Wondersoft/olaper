package org.olap.server.processor.functions;

import java.math.BigDecimal;

import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.Sorter;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.LiteralNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "topcount")
public class TopCount extends AbstractSetFunction {

	public TopCount(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
	}
	
	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer)
			throws OlapException {
		
		BigDecimal topLimit = (BigDecimal) ( (LiteralNode) node.getArgList().get(1) ).getValue();
		ParseTreeNode sortBy = node.getArgList().get(2);

		String sortExpr = buildExpressionString(sortBy, mapping);
		
		layer.setSorter(new Sorter(sortExpr, "DESC"));
		layer.setTopLimit(topLimit.intValue());

		return argument.query(mapping, layer);
		
	}

}
