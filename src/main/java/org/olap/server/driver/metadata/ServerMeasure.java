package org.olap.server.driver.metadata;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;

import org.olap.server.database.CubeMeasureDefinition;
import org.olap4j.OlapException;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Property;

public class ServerMeasure implements Measure {

	private CubeMeasureDefinition definition;
	private Dimension measureDimension;
	private DecimalFormat format;
	
	public ServerMeasure(Dimension measureDimension, CubeMeasureDefinition def) {
		this.definition = def;
		this.measureDimension = measureDimension;
		this.format = def.format==null ? new DecimalFormat("#,##0") : new DecimalFormat(def.format);
	}

	@Override
	public NamedList<? extends Member> getChildMembers() throws OlapException {
		return Olap4jUtil.emptyNamedList();
	}

	@Override
	public int getChildMemberCount() throws OlapException {
		return 0;
	}

	@Override
	public Member getParentMember() {
		return null;
	}

	@Override
	public Level getLevel() {
		return measureDimension.getDefaultHierarchy().getLevels().get(0);
	}

	@Override
	public Hierarchy getHierarchy() {
		return measureDimension.getDefaultHierarchy();
	}

	@Override
	public Dimension getDimension() {
		return measureDimension;
	}

	@Override
	public Type getMemberType() {
		return Type.MEASURE;
	}

	@Override
	public boolean isAll() {
		return false;
	}

	@Override
	public boolean isChildOrEqualTo(Member member) {
		return member==this;
	}

	@Override
	public boolean isCalculated() {
		return false;
	}

	@Override
	public int getSolveOrder() {
		return 0;
	}

	@Override
	public ParseTreeNode getExpression() {
		return null;
	}

	@Override
	public List<Member> getAncestorMembers() {
		return Collections.emptyList();
	}

	@Override
	public boolean isCalculatedInQuery() {
		return false;
	}

	@Override
	public Object getPropertyValue(Property property) throws OlapException {
		if(property==Property.StandardMemberProperty.$visible){
			return new Boolean(isVisible());
		}else if(property==Property.StandardMemberProperty.CHILDREN_CARDINALITY){
			return 0;
		}else if(property==Property.StandardCellProperty.DATATYPE){
			return definition.datatype;
		}else if(property==Property.StandardMemberProperty.MEMBER_UNIQUE_NAME){
			return getUniqueName();
		}else if(property==Property.StandardMemberProperty.MEMBER_CAPTION){
			return getCaption();
		}else if(property==Property.StandardMemberProperty.LEVEL_UNIQUE_NAME){
			return getLevel().getUniqueName();
		}else if(property==Property.StandardMemberProperty.LEVEL_NUMBER){
			return getLevel().getDepth();
		}else{
			return null;
		}
		
	}

	@Override
	public String getPropertyFormattedValue(Property property)
			throws OlapException {
		throw new UnsupportedOperationException("getPropertyFormattedValue");
	}

	@Override
	public void setProperty(Property property, Object value)
			throws OlapException {
		throw new UnsupportedOperationException("setProperty");
	}

	@Override
	public NamedList<Property> getProperties() {
		return Olap4jUtil.emptyNamedList();
	}

	@Override
	public int getOrdinal() {
		return 0;
	}

	@Override
	public boolean isHidden() {
		return false;
	}

	@Override
	public int getDepth() {
		return 0;
	}

	@Override
	public Member getDataMember() {
		throw new UnsupportedOperationException("getDataMember");
	}

	@Override
	public String getName() {
		return definition.name;
	}

	@Override
	public String getUniqueName() {
		return "["+ measureDimension.getName() + "].["+ definition.name +"]";
	}

	@Override
	public String getCaption() {
		return definition.caption;
	}

	@Override
	public String getDescription() {
		return definition.caption;
	}

	@Override
	public Aggregator getAggregator() {
		return Aggregator.valueOf(definition.aggregator);
	}

	@Override
	public Datatype getDatatype() {
		return Datatype.valueOf(definition.datatype);
	}

	@Override
	public boolean isVisible() {
		return true;
	}

	public DecimalFormat getFormat() {
		return format;
	}

}
