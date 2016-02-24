package org.olap.server.processor.functions;

import java.util.Collections;
import java.util.List;

import org.olap.server.processor.LevelMember;
import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap.server.processor.sql.TableMapping.LevelMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Member;

@OlapFunctionName(name = ":")
public class Colon extends OlapFunction {

	private Member member_from, member_to;
	
	public Colon(CallNode node, Cube cube) {
		super(node, cube);
	}

	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {

		List<LevelMemberSet> arg_set_from = createLevelMemberSet(node.getArgList().get(0));
		List<LevelMemberSet> arg_set_to = createLevelMemberSet(node.getArgList().get(1));
		
		member_from = arg_set_from.get(0).getMembers().get(0);
		member_to = arg_set_to.get(0).getMembers().get(0);

		if(member_from.getLevel()!=member_to.getLevel())
			throw new OlapException("Levels mismatch: "+node.toString());
		
		return Collections.singletonList(new LevelMemberSet(member_from.getLevel(), node, this));
	}
	
	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer)
			throws OlapException {
				
		LevelMapping lmap = mapping.getMapping(member_from.getLevel());
		
		return new SetSubquery(lmap.join, lmap.key_column, 
				((LevelMember)member_from).getKey(),
				((LevelMember)member_to).getKey());		
	}


}
