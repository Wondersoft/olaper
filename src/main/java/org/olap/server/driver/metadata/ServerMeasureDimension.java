package org.olap.server.driver.metadata;

import java.util.ArrayList;
import java.util.List;

import org.olap.server.database.CubeMeasureDefinition;
import org.olap.server.driver.util.MetadataUtils;
import org.olap4j.OlapException;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Property;

public class ServerMeasureDimension implements Dimension {

	private static final String NAME = "Measures";
	
	private List<Measure> measures;
	protected ServerMeasureDimension(List<CubeMeasureDefinition> definitions){
		measures = new ArrayList<Measure>();
		for(CubeMeasureDefinition def : definitions){
			measures.add(new ServerMeasure(this, def));
		}
	}
	
	private final Level level = new Level(){

		@Override
		public String getName() {
			return NAME;
		}

		@Override
		public String getUniqueName() {
			return NAME;
		}

		@Override
		public String getCaption() {
			return NAME;
		}

		@Override
		public String getDescription() {
			return NAME;
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
			return hierarchy;
		}

		@Override
		public Dimension getDimension() {
			return ServerMeasureDimension.this;
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

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public List<Member> getMembers() throws OlapException {
			return (List)measures;
		}

		@Override
		public int getCardinality() {
			return measures.size();
		}
		
	};
	
	private final Hierarchy hierarchy = new Hierarchy(){

		@Override
		public String getName() {
			return NAME;
		}

		@Override
		public String getUniqueName() {
			return NAME;
		}

		@Override
		public String getCaption() {
			return NAME;
		}

		@Override
		public String getDescription() {
			return NAME;
		}

		@Override
		public boolean isVisible() {
			return false;
		}

		@Override
		public Dimension getDimension() {
			return ServerMeasureDimension.this;
		}

		@Override
		public NamedList<Level> getLevels() {
			return MetadataUtils.singletoneNamedList(level);
		}

		@Override
		public boolean hasAll() {
			return false;
		}

		@Override
		public Member getDefaultMember() throws OlapException {
			return measures.get(0);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public NamedList<Member> getRootMembers() throws OlapException {
			return (NamedList)MetadataUtils.metadataNamedList(measures);
		}
		
	};
	
	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public String getUniqueName() {
		return NAME;
	}

	@Override
	public String getCaption() {
		return NAME;
	}

	@Override
	public String getDescription() {
		return NAME;
	}

	@Override
	public boolean isVisible() {
		return false;
	}

	@Override
	public NamedList<Hierarchy> getHierarchies() {
		return MetadataUtils.singletoneNamedList(hierarchy);
	}

	@Override
	public Type getDimensionType() throws OlapException {
		return Type.OTHER;
	}

	@Override
	public Hierarchy getDefaultHierarchy() {
		return hierarchy;
	}


}
