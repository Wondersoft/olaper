package org.olap.server.processor.functions;

import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "bottomcount")
public class BottomCount extends TopCount {

	public BottomCount(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
	}
	
	@Override
	public SetSubquery query(TableMapping mapping, LevelMemberSet layer)
			throws OlapException {
		SetSubquery ss = super.query(mapping, layer);
		layer.getSorter().setDirectionString("ASC");
		return ss;
	}

}
