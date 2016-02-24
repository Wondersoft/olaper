package org.olap.server.processor.sql;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.olap.server.database.physical.PhysicalSchema;
import org.olap.server.database.physical.TableJoin;
import org.olap.server.driver.ServerCellSet;
import org.olap.server.driver.metadata.ServerLevel;
import org.olap.server.driver.metadata.ServerMeasure;
import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.ResultAxis;
import org.olap.server.processor.functions.OlapOp;
import org.olap.server.processor.sql.TableMapping.LevelMapping;
import org.olap.server.processor.sql.TableMapping.MeasureMapping;
import org.olap4j.OlapException;
import org.olap4j.metadata.Member;

import com.healthmarketscience.sqlbuilder.Condition;
import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;

public class SqlQuery implements ResultSetProcessor{
	
	private PhysicalSchema schema;
	private ServerCellSet cellSet;
	private SelectQuery sql;
	
	private Set<TableJoin> joins = new HashSet<TableJoin>();
	private List<DbColumn> groupings = new ArrayList<DbColumn>(); 
	private List<Condition> conditions = new ArrayList<Condition>();
	private List<Condition> having_conditions = new ArrayList<Condition>();
	
	private List<ResultAxis> resultAxes;
	private ResultAxis filterAxis;
	private TableMapping mapping;
	
	private static Log log = LogFactory.getLog(SqlQuery.class);
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public SqlQuery(PhysicalSchema schema, ServerCellSet cellSet) {
		
		this.schema = schema;
		this.cellSet = cellSet;
		this.resultAxes = (List)cellSet.getAxes();
		this.filterAxis = (ResultAxis) cellSet.getFilterAxis();
		
	}


	@Override
	public String toString() {			
		return sql.validate().toString();
	}

	public void mapResultToCellSet(ResultSet result) throws SQLException {
		
		List<MeasureMapping> measures = mapping.getMeasures();
		
		while(result.next()){

			for(MeasureMapping measure : measures){
				
				Integer[] position_indexes = new Integer[resultAxes.size()];
				boolean adding_member = true;
				
				for(int i=0; i<resultAxes.size(); i++ ){
					ResultAxis axis = resultAxes.get(i);
					
					List<Member> members = new ArrayList<Member>();
					
					for( LevelMemberSet layer : axis.getLayers() ){
						
						if(layer.isMeasure()){
							members.add(measure.measure);
						}else if(layer.getLevel() instanceof ServerLevel){
							LevelMapping lm = mapping.getMapping(layer.getLevel());
							String dim_value_name = result.getString(lm.name_alias);
							String dim_value_key = lm.key_alias==null ? dim_value_name : result.getString(lm.key_alias);
							Member member = layer.findMember( dim_value_key, dim_value_name);
							if(member!=null)
								members.add(member);
							else
								adding_member = false;
						}
						
						
					}
					
					if(adding_member)
						position_indexes[i] = axis.positionOrdinal(members);
				
				
				}
			
				if(adding_member)
					cellSet.addCell(position_indexes, (Number) result.getObject(measure.alias), measure.measure.getFormat());
				
			}
			
		}
		
	}

	
	public void generateQuery() throws OlapException{
		
		sql = new SelectQuery();
		
		this.mapping = TableMapping.mapToDatabase(schema, resultAxes, filterAxis);
		
		log.debug("SelectQuery using table: "+mapping.getAggregate().table);
		
		buildColumns();
		
		buildSubQueries();
		
		
		for(TableJoin join : joins){
			sql.addJoins(SelectQuery.JoinType.INNER, join.getDbJoin());
		}
		
		if(!groupings.isEmpty()){
			sql.addGroupings(groupings.toArray(new DbColumn[0]));
		}
		
		for(Condition c: conditions)
			sql.addCondition(c);

		for(Condition c: having_conditions)
			sql.addHaving(c);

		addSorters();
		
		
	}


	protected void addSorters() {
		
		for(ResultAxis axis : resultAxes){
			
			List<Sorter> axisSorters = new ArrayList<Sorter>();
			
			boolean breaks_hierarchy = false;
			for(LevelMemberSet layer : axis.getLayers()){
				if(layer.getSorter()!=null){
					axisSorters.add(layer.getSorter());
					if(layer.getSorter().isBreakingHierarchy())
						breaks_hierarchy = true;
				}
			}
			
			if(breaks_hierarchy){
				for(Iterator<Sorter> it = axisSorters.iterator(); it.hasNext();){
					Sorter sorter =  it.next();
					if(sorter.isBelongs_to_hierarchy())
						it.remove();
				}
			}
			
			for(Sorter sorter : axisSorters){
				if(sorter.getColumn()!=null){
					sql.addOrdering(sorter.getColumn(), sorter.getDirection());
				}else{
					sql.addCustomOrdering(sorter.getExpression(), sorter.getDirection());
				}
			}
			
		}
	}


	private void buildSubQueries() throws OlapException {
	
		
		for(ResultAxis axis : resultAxes){
			
			for(LevelMemberSet layer : axis.getLayers()){
				filterLayer(layer);					
			}
			
		}
		
		for(LevelMemberSet layer : filterAxis.getLayers()){
			filterLayer(layer);					
		}
		
		
	}


	protected void filterLayer(LevelMemberSet layer) throws OlapException {
		OlapOp func =layer.getFunction();
		if(func!=null){
			SetSubquery sub_query = func.query(mapping, layer);
			if(sub_query!=null){
				
				Condition condition = sub_query.condition();
				if(condition!=null)
					addCondition(condition);

				Condition having_condition = sub_query.havingCondition();
				if(having_condition!=null)
					addHavingCondition(having_condition);
				
			}
				

		}
	}



	protected void buildColumns() throws OlapException {
		for(ResultAxis axis : resultAxes){
			
			for(LevelMemberSet layer : axis.getLayers()){
				
				if(layer.isMeasure()){
					
					for(Member m : layer.getMembers()){
						TableMapping.MeasureMapping mmap = mapping.getMapping((ServerMeasure) m);						
						sql.addAliasedColumn(mmap.column.getExpression(), mmap.alias);
					}
						
				}else if(layer.getLevel() instanceof ServerLevel){
					
					TableMapping.LevelMapping lmap = mapping.getMapping(layer.getLevel());
										
					sql.addAliasedColumn(lmap.name_column.getDbColumn(), lmap.name_alias);
										
					if(lmap.key_column!=lmap.name_column){
						if(lmap.key_column==null)
							throw new OlapException("Key column for name:"+lmap.name_column.column+" not defined in tables.json, layer "+layer.toString());
						sql.addAliasedColumn(lmap.key_column.getDbColumn(), lmap.key_alias);
						addGroupedBy(lmap.key_column.getDbColumn());
					}

					if(lmap.order_column!=lmap.name_column && lmap.order_column!=lmap.key_column){
						addGroupedBy(lmap.order_column.getDbColumn());
					}
					
					layer.setSorter(new Sorter(lmap.order_column.getDbColumn(), lmap.order_column.sort_direction));
					
					addGroupedBy(lmap.name_column.getDbColumn());
					
					addJoin(lmap.join);					
					
				}
				
			}
			
		}
	}
	

	private void addCondition(Condition condition){
		conditions.add(condition);
	}
	
	private void addHavingCondition(Condition having_condition) {
		having_conditions.add(having_condition);
	}

	
	private void addJoin(TableJoin join){
		joins.add(join);
	}

	
	private void addGroupedBy(DbColumn col){
		if(!groupings.contains(col))
			groupings.add(col);
	}


	public SelectQuery getSql() {
		return sql;
	}


	public TableMapping getMapping() {
		return mapping;
	}


	@Override
	public void process(ResultSet rs) throws SQLException, OlapException {
		mapResultToCellSet(rs);
	}





}
