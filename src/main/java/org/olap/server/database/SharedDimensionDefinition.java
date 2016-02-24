package org.olap.server.database;

import java.util.List;

public class SharedDimensionDefinition extends NamedElement {
	public List<AttributeDefinition> attributes;
	public List<HierarchyDefinition> hierarchies;
}
