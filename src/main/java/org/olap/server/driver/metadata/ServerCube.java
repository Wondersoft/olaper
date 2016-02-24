package org.olap.server.driver.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.olap.server.database.CubeDefinition;
import org.olap.server.driver.util.MetadataUtils;
import org.olap.server.processor.LevelMember;
import org.olap4j.OlapException;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Level.Type;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Member.TreeOp;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.NamedSet;
import org.olap4j.metadata.Schema;

public class ServerCube implements Cube{
	
	private ServerSchema serverSchema;
	private CubeDefinition definition;
	private Dimension measureDimension;
	private NamedList<Dimension> dimensions;
	private NamedList<Hierarchy> hierarchies;
	private List<Measure> measures;
	
	public ServerCube(ServerSchema serverSchema, CubeDefinition def) throws OlapException {
		this.serverSchema = serverSchema;
		this.definition = def;
		this.measureDimension = new ServerMeasureDimension(def.measures);
		readDimensions();
	}

	@Override
	public String getName() {
		return definition.name;
	}

	@Override
	public String getUniqueName() {
		return definition.name;
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
	public Schema getSchema() {
		return serverSchema;
	}

	@Override
	public NamedList<Dimension> getDimensions() {
	   return dimensions;
	}

	private void readDimensions() throws OlapException {
		dimensions = MetadataUtils.metadataNamedList(new ArrayList<Dimension>());
		
		if(definition.dimensions!=null)         
			for(String def : definition.dimensions){
				   Dimension dim = serverSchema.getSharedDimensions().get(def);
				   if(dim!=null)
					   dimensions.add(dim);  
		}
		
		dimensions.add(measureDimension);
	}

	@Override
	public NamedList<Hierarchy> getHierarchies() {
		if(hierarchies!=null)
			return hierarchies;
		hierarchies = MetadataUtils.metadataNamedList(new ArrayList<Hierarchy>());
		
	   return hierarchies;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<Measure> getMeasures() {
		if(measures!=null)
			return measures;
	        
	   try {
		   return (List)measureDimension.getDefaultHierarchy().getLevels().get(0).getMembers();
	   } catch (OlapException e) {
		   return null;
	   }
		
	}

	@Override
	public NamedList<NamedSet> getSets() {
	   return Olap4jUtil.emptyNamedList();
	}

	@Override
	public Collection<Locale> getSupportedLocales() {
		return Collections.emptyList();
	}

	@Override
	public Member lookupMember(List<IdentifierSegment> nameParts)
			throws OlapException {
		
		Dimension dim = getDimensions().get(nameParts.get(0).getName());
		if(dim instanceof ServerMeasureDimension){
			String mp = nameParts.get(1).getName();
			for(Measure m : getMeasures()){
				if(m.getName()!=null && m.getName().equals(mp))
					return m;
			}
		}else if(dim instanceof ServerDimension && nameParts.size()>2){
			Hierarchy h = dim.getHierarchies().get(nameParts.get(1).getName());
			if(h!=null){
				Level level = h.getLevels().get(nameParts.get(2).getName());
				if(level!=null){
					if(level.getLevelType()==Type.ALL)
						return h.getDefaultMember();
					else if(nameParts.size()>3)
						return new LevelMember(level, nameParts.get(3).getName(), 0);
				}
			}
			
		}
		return null;
	}

	@Override
	public List<Member> lookupMembers(Set<TreeOp> treeOps,
			List<IdentifierSegment> nameParts) throws OlapException {
		return Collections.singletonList(lookupMember(nameParts));
	}

	@Override
	public boolean isDrillThroughEnabled() {
		return false;
	}

}
