package org.olap.server.processor.functions;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.olap.server.processor.LevelMemberSet;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "hierarchize")
public class Hierarchize extends CrossJoin {

	public Hierarchize(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
	}

	
	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {
		return sortLevels(super.memberSet());
	}


	private List<LevelMemberSet> sortLevels(List<LevelMemberSet> memberSet) {
		
		Collections.sort(memberSet, new Comparator<LevelMemberSet>(){

			@Override
			public int compare(LevelMemberSet o1, LevelMemberSet o2) {
				
				int d1 = o1.getLevel().getDepth();
				int d2 = o2.getLevel().getDepth();
				
				if(d1<d2)
					return -1;
				else if(d2>d1)
					return 1;
				else
					return 0;
			}
			
		});
		
		return memberSet;
	}

}
