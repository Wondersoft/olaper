package org.olap.server.processor;

import java.util.Collections;
import java.util.List;

import org.olap4j.OlapException;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Property;

public class LevelMember implements Member {

	public static final String NULL_MEMBER = "#null";
	
	private Level level;
	private String name, key;
	private int ordinal;
	
	public LevelMember(Level level, String name, int ordinal){
		String[] comps = name.split("/",2);
		this.level = level;
		this.name = clearControls( (comps.length>1 && !comps[0].isEmpty()) ? comps[1] : comps[0]);
		this.key = clearControls(comps[0]);
		this.ordinal = ordinal;
	}
	
	public LevelMember(Level level, String key,  String name, int ordinal) {
		this.level = level;
		this.name = clearControls(name);
		this.key = clearControls(key);
		this.ordinal = ordinal;
	}

	private static String clearControls(String s){
		return s==null ? s : s.replaceAll("[\\p{C}\\[\\]]", "_");
	}
	
	@Override
	public String getName() {
		if(name!=null && !name.equals(key)){
			return key + "/" + name;
		}else if(key==null){
			return NULL_MEMBER;
		}else
			return key;
	}

	@Override
	public String getUniqueName() {	
		
		return level.getUniqueName() + ".[" + getName() + "]";
	}

	@Override
	public String getCaption() {
		return name==null ? NULL_MEMBER : name;
	}

	@Override
	public String getDescription() {
		return name;
	}

	@Override
	public boolean isVisible() {
		return true;
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
		return level;
	}

	@Override
	public Hierarchy getHierarchy() {
		return level.getHierarchy();
	}

	@Override
	public Dimension getDimension() {
		return level.getDimension();
	}

	@Override
	public Type getMemberType() {
		return Type.REGULAR;
	}

	@Override
	public boolean isAll() {
		return false;
	}

	@Override
	public boolean isChildOrEqualTo(Member member) {
		return false;
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
			return "String";
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
		}	}

	@Override
	public String getPropertyFormattedValue(Property property)
			throws OlapException {
		Object obj = getPropertyValue(property);
		return obj==null ? null : obj.toString();
	}

	@Override
	public void setProperty(Property property, Object value)
			throws OlapException {
	}

	@Override
	public NamedList<Property> getProperties() {
		return null;
	}

	@Override
	public int getOrdinal() {
		return ordinal;
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
		return null;
	}

	public String getKey() {
		return key;
	}

}
