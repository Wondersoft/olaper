package org.olap.server.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.olap.server.driver.ServerCellSetAxis;
import org.olap.server.driver.metadata.ServerDimension;
import org.olap.server.driver.metadata.ServerMeasure;
import org.olap.server.processor.functions.OlapFunctionFactory;
import org.olap4j.CellSet;
import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.mdx.AxisNode;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Member;

public class ResultAxis extends ServerCellSetAxis {

	private List<LevelMemberSet> layers;
	private Map<List<Member>, Position> member_map = new HashMap<List<Member>, Position>();
	private List<Position> positions = new ArrayList<Position>();
	
	public ResultAxis(CellSet cellSet, AxisNode axisNode) throws OlapException {
		super(cellSet, axisNode.getAxis());		
		this.layers = OlapFunctionFactory.INSTANCE.function( axisNode.getExpression(), cellSet.getMetaData().getCube()).memberSet();
	}

	@Override
	public List<Position> getPositions() {
		return positions;
	}

	@Override
	protected List<Hierarchy> getHierarchies() {
		List<Hierarchy> hierarchies = new ArrayList<Hierarchy>();

		for(LevelMemberSet layer : layers){
			Hierarchy h = layer.getLevel().getHierarchy();
			if(!hierarchies.contains(h))
				hierarchies.add(h);
		}
		
		return hierarchies;
	}



	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(getAxisMetaData().getAxisOrdinal().name());
		
		sb.append(" with ").append(layers.size()).append(" layer(s): ");
		for(LevelMemberSet set: layers){
			sb.append("\n");
			sb.append(set.toString());
		}
		
		return sb.toString();
	}

	public List<LevelMemberSet> getLayers() {
		return layers;
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Set<ServerMeasure> collectAllUsedMeasures() throws OlapException{
		
		Set measures = new HashSet();
		for(LevelMemberSet layer : layers){
			if(layer.isMeasure()){
				measures.addAll(layer.getMembers());
			}
			measures.addAll(layer.getFunction().getAllMeasuresUsed());
		}
		
		return measures;
		
	}
	
	public Set<ServerDimension> collectAllUsedDimensions(){
		Set<ServerDimension> dims = new HashSet<ServerDimension>();
		for(LevelMemberSet layer : layers){
			if(!layer.isMeasure()){
				dims.add((ServerDimension) layer.getLevel().getDimension());
			}
			
		}
		
		return dims;		
	}

	public int positionOrdinal(final List<Member> members) {
		
		Position p = member_map.get(members);
		 
		if(p==null){
			final int ordinal = positions.size();
			p = new Position(){

				@Override
				public List<Member> getMembers() {
					return members;
				}

				@Override
				public int getOrdinal() {				
					return ordinal;
				}
				
			};
			member_map.put(members, p);
			positions.add(p);
		}
		
		return p.getOrdinal();
		
	}
	
	
	
}
