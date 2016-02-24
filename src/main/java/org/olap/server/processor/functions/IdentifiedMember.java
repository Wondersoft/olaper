package org.olap.server.processor.functions;

import java.util.Collections;
import java.util.List;

import org.olap.server.driver.util.MetadataUtils;
import org.olap.server.driver.util.ParseUtils;
import org.olap.server.processor.LevelMember;
import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap.server.processor.sql.TableMapping.LevelMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Member;

public class IdentifiedMember extends OlapMethod {

	private Member cube_member;
	
	public IdentifiedMember(IdentifierNode node, Cube cube) throws OlapException {
		super(node, cube);
		
		cube_member = cube.lookupMember(node.getSegmentList());
		
		if(cube_member==null)
			cube_member = MetadataUtils.lookupMember(cube, node.getSegmentList());
		
		if(cube_member==null)
			throw new OlapException("Member not found in cube: "+ParseUtils.toString(node));

	}


	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {
		return Collections.singletonList(new LevelMemberSet(cube_member, node, this));
	}

	
	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer)
			throws OlapException {
		
		if(! (cube_member instanceof LevelMember))
			return null;
		
		LevelMember lmember = (LevelMember) cube_member;
		
		LevelMapping lmap = mapping.getMapping(cube_member.getLevel());
		
		return new SetSubquery(lmap.join, lmap.key_column, Collections.singletonList(lmember.getKey()));		
	}


	public Member getCube_member() {
		return cube_member;
	}


}
