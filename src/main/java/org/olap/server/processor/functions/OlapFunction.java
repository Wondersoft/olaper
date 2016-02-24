package org.olap.server.processor.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public abstract class OlapFunction implements OlapOp {

	protected CallNode node;
	protected Cube cube;
	
	public OlapFunction(CallNode node, Cube cube){
		this.node = node;
		this.cube = cube;
	}
	
	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer) throws OlapException{		
		return null;
	}
	
	@Override
	public Collection<ServerMeasure> getAllMeasuresUsed() throws OlapException {
		if(node instanceof CallNode){
			Set<ServerMeasure> result = new HashSet<ServerMeasure>();
			for(IdentifierNode inode : ParseUtils.getAllIdentifiers((CallNode) node)){
				Member m = cube.lookupMember(inode.getSegmentList());
				if(m instanceof ServerMeasure)
					result.add((ServerMeasure) m);
			}
			return result;
		}else
			return Collections.emptyList();
	}
	
	protected List<LevelMemberSet> createLevelMemberSet(ParseTreeNode node) throws OlapException{
		return  OlapFunctionFactory.INSTANCE.function( node, cube).memberSet();
	}



}
