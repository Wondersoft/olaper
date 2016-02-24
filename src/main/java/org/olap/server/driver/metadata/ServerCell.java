package org.olap.server.driver.metadata;

import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.List;

import org.olap4j.AllocationPolicy;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapException;
import org.olap4j.metadata.Property;

public class ServerCell implements Cell {

	private CellSet cellSet;
	private List<Integer> coordinates;
	private Number value;
	private DecimalFormat format;
	
	public ServerCell(CellSet cellSet, List<Integer> coordinates, Number value, DecimalFormat format){
		this.cellSet = cellSet;
		this.coordinates = coordinates;
		this.value = value;
		this.format = format;
	}
	
	@Override
	public CellSet getCellSet() {
		return cellSet;
	}

	@Override
	public int getOrdinal() {
		return cellSet.coordinatesToOrdinal(coordinates);
	}

	@Override
	public List<Integer> getCoordinateList() {
		return coordinates;
	}

	@Override
	public Object getPropertyValue(Property property) {
		// VALUE FORMATTED_VALUE FORMAT_STRING 
		if(property==Property.StandardCellProperty.VALUE){
			return getValue();
		}else if(property==Property.StandardCellProperty.FORMATTED_VALUE){
			return getFormattedValue();
		}
		return null;
	}

	@Override
	public boolean isEmpty() {
		return value==null;
	}

	@Override
	public boolean isError() {
		return false;
	}

	@Override
	public boolean isNull() {
		return value==null;
	}

	@Override
	public double getDoubleValue() throws OlapException {
		return value==null ? 0.0 : value.doubleValue();
	}

	@Override
	public String getErrorText() {
		return null;
	}

	@Override
	public Object getValue() {
		return value;
	}

	@Override
	public String getFormattedValue() {
		return value==null ? "null" : (format==null ? value.toString() : format.format(value));
	}

	@Override
	public ResultSet drillThrough() throws OlapException {
		throw new UnsupportedOperationException("drillThrough not suported for Cell");
	}

	@Override
	public void setValue(Object value, AllocationPolicy allocationPolicy,
			Object... allocationArgs) throws OlapException {
		throw new UnsupportedOperationException("setValue not suported for Cell");

	}

}
