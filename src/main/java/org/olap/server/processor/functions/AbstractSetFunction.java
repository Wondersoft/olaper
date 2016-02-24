package org.olap.server.processor.functions;

import java.util.List;

import org.olap.server.driver.metadata.ServerMeasure;
import org.olap.server.driver.util.ParseUtils;
import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Member;

public abstract class AbstractSetFunction extends OlapFunction {

	protected OlapOp argument;
	public AbstractSetFunction(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
		if(node.getArgList().size()<1)
			throw new OlapException("Expected at least one argument: "+node.toString());
		this.argument = OlapFunctionFactory.INSTANCE.function( node.getArgList().get(0), cube);
	}

	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {
		List<LevelMemberSet> set = argument.memberSet();
		for(LevelMemberSet s : set)
			s.setFunction(this);
		return set;
	}

	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer)
			throws OlapException {
		return argument.query(mapping, layer);		
	}
	
	protected String buildExpressionString(ParseTreeNode expressionNode, TableMapping mapping) throws OlapException{
		
		if(expressionNode instanceof IdentifierNode){
			return getExpression((IdentifierNode) expressionNode, mapping, true);			
		}else if(expressionNode instanceof CallNode){
			String expression = ParseUtils.toString(expressionNode);			
			for(IdentifierNode inode : ParseUtils.getAllIdentifiers((CallNode) expressionNode)){
				expression = expression.replace(inode.toString(), getExpression(inode, mapping, false));
			}
			return expression;
		}else
			throw new OlapException("Unexpected expression node "+expressionNode.toString());
	}

	private String getExpression(IdentifierNode inode, TableMapping mapping, boolean useAliases)
			throws OlapException {
		Member m = cube.lookupMember(inode.getSegmentList());
		
		if(m instanceof ServerMeasure){
			return mapping.getMeasureExpression((ServerMeasure) m, useAliases);
		}else
			throw new OlapException("Can not expression by "+inode.toString());
	}
	
}
