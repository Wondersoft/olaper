package org.olap.server.database;

import java.util.List;

public class CubeDefinition  extends NamedElement {
	public List<String> dimensions;
	public List<CubeMeasureDefinition> measures;	
}
