package org.olap.server.processor.functions;

import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.metadata.Cube;

@OlapFunctionName(name = "*")
public class Star extends CrossJoin {

	public Star(CallNode node, Cube cube) throws OlapException {
		super(node, cube);
	}

}
