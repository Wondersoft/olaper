package org.olap.server.processor.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.olap.server.driver.metadata.ServerLevel;
import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap.server.processor.sql.TableMapping.LevelMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "{}")
public class CurlyBrackets extends AbstractArgFunction {

	public CurlyBrackets(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
	}


	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {
		
		List<LevelMemberSet> result = new ArrayList<LevelMemberSet>();
		
		for(OlapOp arg: arguments ) {
			
			List<LevelMemberSet> arg_set = arg.memberSet();
			
			for(LevelMemberSet layer: arg_set){
				if(layer.getMembers().size()>0){				
					append_members(result, layer);				
				}else{
					result.add(layer);
				}
			}
			
			
		}
		
		return result;
	}

	private void append_members(List<LevelMemberSet> result, LevelMemberSet layer) {
		
		for(LevelMemberSet set : result){
			
			if(set.getLevel()==layer.getLevel()){
				set.getMembers().addAll(layer.getMembers());
				return;
			}
			
		}
		
		LevelMemberSet set = new LevelMemberSet(layer.getLevel(), node, this);
		set.getMembers().addAll(layer.getMembers());
		result.add(set);
		
	}


	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer) throws OlapException {
		
		if(layer.isMeasure() || !(layer.getLevel() instanceof ServerLevel))
			return null;
		
		LevelMapping lmap = mapping.getMapping(layer.getLevel());
		SetSubquery query = new SetSubquery(lmap.join, lmap.key_column, Collections.<String> emptyList());
		
		for(OlapOp arg: arguments ) {
			SetSubquery q = arg.query(mapping, layer);
			if(q!=null)
				query.or(q);
				
		}
		
		return query;
	}

}
