package org.olap.server.processor.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.olap.server.database.physical.DimensionTable;
import org.olap.server.database.physical.PhysicalSchema;
import org.olap.server.database.physical.TableColumn;
import org.olap.server.processor.LevelMember;
import org.olap4j.OlapException;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;

import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.custom.mysql.MysLimitClause;

public class SqlMembersQuery implements ResultSetProcessor {

	private PhysicalSchema physicalSchema;
	private Level level;
	private List<Member> members;
	
	private static String ID_ALIAS = "id0";
	private static String NAME_ALIAS = "name0";

	
	private SelectQuery sql;
	
	public SqlMembersQuery(PhysicalSchema physicalSchema, Level level) {
		this.physicalSchema = physicalSchema;
		this.level = level;
	}
	
	public void generateQuery() throws OlapException {
		
		DimensionTable dtable = physicalSchema.findDimensionTable(level.getDimension());
		if(dtable==null)
			throw new OlapException("Not found dimension mapping: "+level.getDimension());
		TableColumn column = dtable.findColumnByAttribute(level.getName());
		if(column==null)
			throw new OlapException("Not found column mapping: "+level.getName());
		
		TableColumn id_column = column.identified_by==null ? column : dtable.findColumnByName(column.identified_by);
		TableColumn sort_column = column.sorted_by==null ? column : dtable.findColumnByName(column.sorted_by);
		
		
		sql = new SelectQuery();
		
		sql.addAliasedColumn(id_column.getDbColumn(), ID_ALIAS);
		sql.addAliasedColumn(column.getDbColumn(), NAME_ALIAS);		
		
		sql.addGroupings(id_column.getDbColumn());	
		
		if(id_column!=column){
			sql.addGroupings(column.getDbColumn());
		}
		
		if(sort_column!=column && sort_column!=id_column){
			sql.addGroupings(sort_column.getDbColumn());
		}
		
		sql.addOrderings(sort_column.getDbColumn());		
		sql.addCustomization(new MysLimitClause(1000));

		
	}

	@Override
	public void process(ResultSet result) throws SQLException, OlapException {
		members = new ArrayList<Member>();
		while(result.next()){
			
			String key = result.getString(ID_ALIAS);
			String name = result.getString(NAME_ALIAS);
			
			members.add(new LevelMember(level, key, name, members.size()));
			
		}
	}



	public List<Member> getMembers() {
		return members;
	}
	
	@Override
	public String toString() {
		return sql.validate().toString();
	}

}
