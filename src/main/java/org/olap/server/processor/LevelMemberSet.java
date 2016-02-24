package org.olap.server.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.olap.server.driver.metadata.ServerMeasureDimension;
import org.olap.server.driver.util.ParseUtils;
import org.olap.server.processor.functions.OlapOp;
import org.olap.server.processor.sql.Sorter;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;

public class LevelMemberSet {
	
	private Level level;
	
	private ParseTreeNode node;
	private OlapOp function;
	private List<Member> members = new ArrayList<Member>();
	private Map<String, Member> map = new HashMap<String, Member>();
	
	private Sorter sorter;
	private int topLimit;
	
	public LevelMemberSet(Level level, ParseTreeNode node, OlapOp function) {
		this.level = level;
		this.node = node;
		this.function = function;
	}

	public LevelMemberSet(Member member, ParseTreeNode node, OlapOp function) {		
		this.level = member.getLevel();
		this.node = node;
		this.function = function;
		members.add(member);		
	}

	public LevelMemberSet(ParseTreeNode node, OlapOp function) {
		this.node = node;
		this.function = function;
	}

	public Level getLevel() {
		return level;
	}
	
	public boolean isMeasure(){
		return level.getDimension() instanceof ServerMeasureDimension;
	}

	public ParseTreeNode getNode() {
		return node;
	}

	public List<Member> getMembers() {
		return members;
	}

		
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(level.getUniqueName());
		
		if(!members.isEmpty()){
			sb.append(" with ").append(members.size()).append(" member(s): [");
			for(Member m: members){
				if(m!=members.get(0))
					sb.append(",");
				sb.append(m.getName());
			}
			sb.append("]");
		}
		
		sb.append(" node: ");
		sb.append(ParseUtils.toString(node));

		return sb.toString();
	}

	public OlapOp getFunction() {
		return function;
	}

	public Member findMember(String key, String name) {
		
		Member m = map.get(key);
		if(m!=null)
			return m;
		
		if(topLimit>0 && members.size()>=topLimit)
			return null;
		
		LevelMember member = new LevelMember(level, key, name, members.size());
		members.add(member);
		map.put(key, member);
		
		return member;
	}

	public void setFunction(OlapOp function) {
		this.function = function;
	}

	public Sorter getSorter() {
		return sorter;
	}

	public void setSorter(Sorter sorter) {
		this.sorter = sorter;
	}

	public int getTopLimit() {
		return topLimit;
	}

	public void setTopLimit(int topLimit) {
		this.topLimit = topLimit;
	}
	
	
	
}
