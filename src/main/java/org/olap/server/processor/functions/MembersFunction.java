package org.olap.server.processor.functions;

import java.util.Collections;
import java.util.List;

import org.olap.server.driver.util.MetadataUtils;
import org.olap.server.driver.util.ParseUtils;
import org.olap.server.processor.LevelMemberSet;
import org.olap4j.OlapException;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Level;

@OlapFunctionName(name = "members")
public class MembersFunction extends OlapMethod {


	public MembersFunction(IdentifierNode node, Cube cube) {
		super(node, cube);
	}


	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {

		Level level = MetadataUtils.lookupLevel(cube, node.getSegmentList());
		if(level==null)
			throw new OlapException("Level not found in cube: "+ParseUtils.toString(node));
		
		return Collections.singletonList(new LevelMemberSet(level, node, this));
	}
	
	


}
