package org.olap.server.processor.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap.server.database.physical.AggregateTable;
import org.olap.server.database.physical.DimensionTable;
import org.olap.server.database.physical.PhysicalSchema;
import org.olap.server.database.physical.TableColumn;
import org.olap.server.database.physical.TableJoin;
import org.olap.server.database.physical.TableMeasure;
import org.olap.server.driver.metadata.LiteralMeasure;
import org.olap.server.driver.metadata.ServerDimension;
import org.olap.server.driver.metadata.ServerLevel;
import org.olap.server.driver.metadata.ServerMeasure;
import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.ResultAxis;
import org.olap4j.OlapException;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;

import com.healthmarketscience.sqlbuilder.NumberValueObject;
import com.healthmarketscience.sqlbuilder.ValueObject;

public class TableMapping {
	
	private static Log log = LogFactory.getLog(SqlConnector.class);
	
	public class MeasureMapping {
		public ServerMeasure measure;
		public TableMeasure column;
		public String alias;
	}

	public class LevelMapping {
		public TableJoin join;
		public DimensionTable table;
		public TableColumn name_column;
		public String name_alias;
		
		public TableColumn key_column;
		public String key_alias;
		
		public TableColumn order_column;
	}
		
	private AggregateTable aggregate;
	
	private List<MeasureMapping> measures = new ArrayList<MeasureMapping>();
	
	private Map<ServerMeasure, MeasureMapping> measures_mapping = new IdentityHashMap<ServerMeasure, MeasureMapping>();
	private Map<Level, LevelMapping> levels_mapping = new IdentityHashMap<Level, LevelMapping>();	
	
	public TableMapping(AggregateTable t) {
		this.aggregate = t;
	}


	public static TableMapping mapToDatabase(PhysicalSchema schema, List<ResultAxis> resultAxes, ResultAxis filterAxis) throws OlapException {
		
		Set<ServerMeasure> measures = new HashSet<ServerMeasure>();
		Set<ServerDimension> dimensions = new HashSet<ServerDimension>();
		
		for(ResultAxis axis : resultAxes){
			measures.addAll(axis.collectAllUsedMeasures());
			dimensions.addAll(axis.collectAllUsedDimensions());
		}

		measures.addAll(filterAxis.collectAllUsedMeasures());
		dimensions.addAll(filterAxis.collectAllUsedDimensions());

		List<TableMapping> candidates = new ArrayList<TableMapping>();
		
		for(AggregateTable t : schema.aggregates){
			TableMapping mapping = new TableMapping(t);
			
			boolean all_mapped = true;
			for(ServerMeasure m : measures){
				if(mapping.map(m)==null){
					all_mapped = false;
					log.info("Can not use "+t.table + " missing "+m.getName()+" mapping");
					break;
				}
			}
			
			if(all_mapped){
				for(ServerDimension d : dimensions){
					if(mapping.map(d)==null){
						all_mapped = false;
						log.info("Can not use "+t.table + " missing "+d.getName()+" mapping");
						break;
					}
				}
				
			}
			
			if(all_mapped)
				candidates.add(mapping);
			
			
		}
		
		
		Collections.sort(candidates, new Comparator<TableMapping>(){

			@Override
			public int compare(TableMapping o1, TableMapping o2) {
				if(o1.scale() > o2.scale())
					return 1;
				else if(o1.scale() < o2.scale())
					return -1;
				else
					return 0;
			}
			
		});
		
		TableMapping mapping = candidates.get(0);
		log.info("Using "+ mapping.aggregate.table+" aggregate table");
		
		for(ResultAxis axis : resultAxes){
			for( LevelMemberSet layer : axis.getLayers() ){
				mapping.map(layer);
			}
		}
		
		for( LevelMemberSet layer : filterAxis.getLayers() ){
			mapping.map(layer);
		}
		
		
		return mapping;
	}
	


	private void map(LevelMemberSet layer) throws OlapException {
		
		if(layer.isMeasure()){
			
			for(Member m : layer.getMembers()){
				ServerMeasure sm = (ServerMeasure) m;
				MeasureMapping mm = new MeasureMapping();
				mm.alias = "m"+(measures_mapping.size() + 1);
				mm.column = map(sm);
				mm.measure = sm;
				measures_mapping.put(sm, mm );
				measures.add(mm);
			}
			
		}else if(layer.getLevel() instanceof ServerLevel && !levels_mapping.containsKey(layer.getLevel())){
							
			LevelMapping lm = new LevelMapping();
			TableJoin join = map(layer.getLevel().getDimension());

			lm.join = join;
			lm.name_column = join.getDimensionTable().findColumnByAttribute(layer.getLevel().getName());
			if(lm.name_column==null)
				throw new OlapException("Column for "+layer.getLevel().getName()+" not found in "+join.getDimensionTable().table);
			
			lm.name_alias = "d"+(levels_mapping.size() + 1);
			
			if(lm.name_column.identified_by!=null){
				lm.key_column = join.getDimensionTable().findColumnByName(lm.name_column.identified_by);
				lm.key_alias = "id"+(levels_mapping.size() + 1);
			}else{
				lm.key_column = lm.name_column;
				lm.key_alias = lm.key_alias;
			}
			
			if(lm.name_column.sorted_by!=null){
				lm.order_column = join.getDimensionTable().findColumnByName(lm.name_column.sorted_by);
			}else{
				lm.order_column = lm.name_column;
			}
			
			
			levels_mapping.put(layer.getLevel(), lm);
			
			
		}
		
	}


	private TableJoin map(Dimension d) {
		for(TableJoin join : aggregate.joins){
			if(join.dimension.equals(d.getUniqueName())){
				return join;
			}
		}
		return null;
	}


	private TableMeasure map(ServerMeasure m) {
		
		if(m instanceof LiteralMeasure){
			LiteralMeasure literal = (LiteralMeasure) m;
			TableMeasure literalMeasure = new TableMeasure();
			literalMeasure.setExpression(
					literal.isNumeric() ? 
						new NumberValueObject(literal.getLiteralValue()) : 
						new ValueObject(literal.getLiteralValue())
						);
			return literalMeasure;
		}
		
		for(TableMeasure measure : aggregate.measures){
			if(measure.measure.equals(m.getName())){
				return measure;
			}
		}
		return null;
	}

	public String getMeasureExpression(ServerMeasure measure, boolean useAliases){
		MeasureMapping mapping = getMapping(measure);
		
		if(mapping!=null)
			return useAliases ? mapping.alias : mapping.column.getExpression().toString();
		else{
			TableMeasure tm = map(measure);
			return tm.getExpression().toString();
		}	
	}


	public int scale() {		
		return aggregate.joins.size();
	}


	public AggregateTable getAggregate() {
		return aggregate;
	}
	
	public MeasureMapping getMapping(ServerMeasure measure){
		return measures_mapping.get(measure);
	}

	public LevelMapping getMapping(Level level){
		return levels_mapping.get(level);
	}
	
	public Set<TableMeasure> getMeasureColumns(){
		return null;		
	}

	public Set<TableColumn> getTableColumns(){
		return null;		
	}

	public Set<TableJoin> getTableJoins(){
		return null;		
	}


	public List<MeasureMapping> getMeasures() {
		return measures;
	}
}
