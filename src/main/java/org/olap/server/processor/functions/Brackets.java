package org.olap.server.processor.functions;

import java.util.List;

import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "()")
public class Brackets extends AbstractArgFunction{
	
	public Brackets(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
	}

	@Override
	public List<LevelMemberSet> memberSet() throws OlapException {
		return arguments.get(0).memberSet();
	}
	
	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer)
			throws OlapException {
		return arguments.get(0).query(mapping, layer);
	}

}
