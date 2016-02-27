package org.olap.server.driver.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.olap.server.database.AttributeDefinition;
import org.olap.server.database.HierarchyDefinition;
import org.olap.server.database.NamedElement;
import org.olap.server.database.SharedDimensionDefinition;
import org.olap.server.driver.util.MetadataUtils;
import org.olap4j.OlapException;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.NamedList;

public class ServerDimension implements Dimension{

	private SharedDimensionDefinition definition;
	private NamedList<Hierarchy> hierarchies;
	private ServerSchema serverSchema;
	
	public ServerDimension( SharedDimensionDefinition def, ServerSchema serverSchema) throws OlapException {
		this.definition = def;
		this.serverSchema = serverSchema;
		readHierarchies();
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
	public NamedList<Hierarchy> getHierarchies() {
		return hierarchies;
	}

	private void readHierarchies() throws OlapException {
		
		hierarchies = MetadataUtils.metadataNamedList(new ArrayList<Hierarchy>());  
		
		Map<String, AttributeDefinition> attributeMap = new HashMap<String, AttributeDefinition>();
		
	    if(definition.attributes!=null)        
	    for(AttributeDefinition cdef: definition.attributes){
	    	attributeMap.put(cdef.name, cdef);
	    	if(cdef.auto_hierarchy){
	    		hierarchies.add(new ServerHierarchy(this, cdef,  Collections.singletonList((NamedElement)cdef)));
	    	}
	    }
	    
	    if(definition.hierarchies!=null)
	    for(HierarchyDefinition hdef : definition.hierarchies){
	    	
	    	List<NamedElement> levelDefs = new ArrayList<NamedElement>();
	    	for(String level : hdef.levels){	
	    		AttributeDefinition attr_def = attributeMap.get(level);
	    		if(attr_def==null)
	    			throw new OlapException("Fail to find attribute "+level+" used in dimension "+ getName());
	    		levelDefs.add(attr_def);
	    	}
	    	
	    	hierarchies.add(new ServerHierarchy(this, hdef, levelDefs));
	    }
	}

	@Override
	public Type getDimensionType() throws OlapException {
		return Type.UNKNOWN;
	}

	@Override
	public Hierarchy getDefaultHierarchy() {
		throw new UnsupportedOperationException("getDefaultHierarchy");
	}

	public ServerSchema getServerSchema() {
		return serverSchema;
	}

}
