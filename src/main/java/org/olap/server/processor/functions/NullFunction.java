package org.olap.server.processor.functions;

import java.util.ArrayList;
import java.util.List;

import org.olap.server.processor.LevelMemberSet;
import org.olap4j.OlapException;
import org.olap4j.metadata.Cube;


public class NullFunction extends OlapFunction {

	public NullFunction(Cube cube) {
		super(null, cube);
	}

	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {
		return new ArrayList<LevelMemberSet>();
	}

}
