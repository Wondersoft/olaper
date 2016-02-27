package org.olap.server.driver.metadata;

import java.util.List;

import org.olap.server.database.NamedElement;
import org.olap.server.processor.MemberQuery;
import org.olap4j.OlapException;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Property;

public class ServerLevel implements Level {

	private ServerHierarchy hierarchy;
	private NamedElement definition;
	private int depth;
	
	protected ServerLevel(ServerHierarchy hierarchy, NamedElement def, int depth){
		this.hierarchy = hierarchy;
		this.definition = def;
	}
	
	
	@Override
	public String getName() {
		return definition.name;
	}

	@Override
	public String getUniqueName() {
		return hierarchy.getUniqueName() +".["+definition.name+"]";
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
	public boolean isVisible() {
		return true;
	}

	@Override
	public int getDepth() {
		return depth;
	}

	@Override
	public Hierarchy getHierarchy() {
		return hierarchy;
	}

	@Override
	public Dimension getDimension() {
		return hierarchy.getDimension();
	}

	@Override
	public Type getLevelType() {
		return Type.REGULAR;
	}

	@Override
	public boolean isCalculated() {
		return false;
	}

	@Override
	public NamedList<Property> getProperties() {
		return Olap4jUtil.emptyNamedList();
	}

	@Override
	public List<Member> getMembers() throws OlapException {
		ServerDimension dim = (ServerDimension) getDimension();		
		MemberQuery query = new MemberQuery(dim.getServerSchema().getCatalog(), this);
		return query.getMembers();
	}

	@Override
	public int getCardinality() {
		return 0;
	}
}
