package org.olap.server.driver.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.olap.server.database.NamedElement;
import org.olap.server.driver.util.MetadataUtils;
import org.olap4j.OlapException;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Property;

public class ServerHierarchy implements Hierarchy {

	private ServerDimension serverDimension;
	private NamedElement definition;
	private NamedList<Level> levels;
	
	private Level allLevel = new Level(){

		@Override
		public String getName() {
			return "All";
		}

		@Override
		public String getUniqueName() {
			return ServerHierarchy.this.getUniqueName() +".[All "+ServerHierarchy.this.getName()+"]";
		}

		@Override
		public String getCaption() {
			return "(All)";
		}

		@Override
		public String getDescription() {
			return getName();
		}

		@Override
		public boolean isVisible() {
			return false;
		}

		@Override
		public int getDepth() {
			return 0;
		}

		@Override
		public Hierarchy getHierarchy() {
			return ServerHierarchy.this;
		}

		@Override
		public Dimension getDimension() {
			return ServerHierarchy.this.getDimension();
		}

		@Override
		public Type getLevelType() {
			return Type.ALL;
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
			return Collections.emptyList();
		}

		@Override
		public int getCardinality() {
			return 0;
		}
		
	};
	
	private Member all = new Member(){

		private final static String NAME = "All";
		
		@Override
		public String getName() {
			return NAME;
		}

		@Override
		public String getUniqueName() {
			return ServerHierarchy.this.getUniqueName() + ".[" + NAME +  "]";
		}

		@Override
		public String getCaption() {
			return NAME + " " + ServerHierarchy.this.getName();
		}

		@Override
		public String getDescription() {
			return NAME;
		}

		@Override
		public boolean isVisible() {
			return true;
		}

		@Override
		public NamedList<? extends Member> getChildMembers()
				throws OlapException {
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
			return levels.get(0);
		}

		@Override
		public Hierarchy getHierarchy() {
			return null;
		}

		@Override
		public Dimension getDimension() {
			return ServerHierarchy.this.getDimension();
		}

		@Override
		public Type getMemberType() {
			return Type.ALL;
		}

		@Override
		public boolean isAll() {
			return true;
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
			}	
		}


		@Override
		public String getPropertyFormattedValue(Property property)
				throws OlapException {
			return null;
		}

		@Override
		public void setProperty(Property property, Object value)
				throws OlapException {
			
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
			return null;
		}
		
	};
	
	public ServerHierarchy(ServerDimension serverDimension, NamedElement cdef, 
			List<NamedElement> levelDefs) {
		this.serverDimension = serverDimension;
		this.definition  = cdef;
	 	this.levels =  MetadataUtils.metadataNamedList(new ArrayList<Level>()); 
	 	
	 	
	 	levels.add(allLevel);
	 	int depth = 1;
	 	for(NamedElement attr_def : levelDefs){
	 		levels.add(new ServerLevel(this, attr_def, depth++));
	 	}
	 	
	}

	@Override
	public String getName() {
		return definition.name;
	}

	@Override
	public String getUniqueName() {
		return "["+ serverDimension.getName() + "].["+ definition.name +"]";
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
	public Dimension getDimension() {
		return serverDimension;
	}

	@Override
	public NamedList<Level> getLevels() {
		return levels;
	}

	@Override
	public boolean hasAll() {
		return true;
	}

	@Override
	public Member getDefaultMember() throws OlapException {
		return all;
	}

	@Override
	public NamedList<Member> getRootMembers() throws OlapException {
		return MetadataUtils.singletoneNamedList(all);
	}
	
	@Override
	public int hashCode() {
		return getUniqueName().hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof Hierarchy) )
			return true;
		Hierarchy h = (Hierarchy) obj;
		return getUniqueName().equals(h.getUniqueName());
	}

}
