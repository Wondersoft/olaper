package org.olap.server.driver.metadata;

import org.olap.server.database.CubeMeasureDefinition;
import org.olap4j.mdx.LiteralNode;
import org.olap4j.metadata.Dimension;
import org.olap4j.type.NumericType;

public class LiteralMeasure extends ServerMeasure {

	private LiteralNode node;
	public LiteralMeasure(Dimension measureDimension, LiteralNode node) {
		super(measureDimension, definition(node));
		this.node = node;
	}

	private static CubeMeasureDefinition definition(LiteralNode node) {
		CubeMeasureDefinition def = new CubeMeasureDefinition();
		def.name = node.getValue().toString();
		return def;
	}
	
	public boolean isNumeric(){
		return node.getType() instanceof NumericType;
	}
	
	public Object getLiteralValue(){
		return isNumeric() ? Float.parseFloat(node.getValue().toString()) : node.getValue();
	}
	
	

}
