package org.olap.server.processor.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "crossjoin")
public class CrossJoin extends OlapFunction {

	private Map<LevelMemberSet, OlapOp> nodeMap; 
	
	public CrossJoin(CallNode node, Cube cube) {
		super(node, cube);
	}

	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {
		
		nodeMap = new HashMap<LevelMemberSet, OlapOp>();
		List<LevelMemberSet> sets = new ArrayList<LevelMemberSet>();

		for(ParseTreeNode arg: node.getArgList() ) {
			
			OlapOp op = OlapFunctionFactory.INSTANCE.function( arg, cube);
			List<LevelMemberSet> arg_set = op.memberSet();
						
			sets.addAll(arg_set);
			
			for(LevelMemberSet s : arg_set){
				nodeMap.put(s, op);
			}
			
		}
		
		return sets;
	}

	
	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer)
			throws OlapException {
		OlapOp op = nodeMap.get(layer);		
		return op.query(mapping, layer);		
	}
}
