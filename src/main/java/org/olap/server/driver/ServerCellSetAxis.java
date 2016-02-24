package org.olap.server.driver;

import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.olap4j.Axis;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.CellSetAxisMetaData;
import org.olap4j.Position;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Property;

public abstract class ServerCellSetAxis implements CellSetAxis {

	private CellSet cellSet;
	private Axis ordinal;
	private CellSetAxisMetaData metaData;
	
	public ServerCellSetAxis(	CellSet cellSet, 
								Axis ordinal){

		this.ordinal = ordinal;
		this.cellSet = cellSet;
		
		
		this.metaData = new CellSetAxisMetaData(){

			@Override
			public Axis getAxisOrdinal() {
				return ServerCellSetAxis.this.getAxisOrdinal();
			}

			@Override
			public List<Hierarchy> getHierarchies() {
				return ServerCellSetAxis.this.getHierarchies();
			}

			@Override
			public List<Property> getProperties() {
				return Collections.emptyList();
			}
			
		};
		
		
	}
	
	
	@Override
	public Axis getAxisOrdinal() {	
		return ordinal;
	}

	@Override
	public CellSet getCellSet() {
		return cellSet;
	}

	@Override
	public CellSetAxisMetaData getAxisMetaData() {
		return metaData;
	}

	
	protected abstract List<Hierarchy> getHierarchies();

	@Override
	public int getPositionCount() {
		return getPositions().size();
	}

	@Override
	public ListIterator<Position> iterator() {
		return getPositions().listIterator();
	}

}
