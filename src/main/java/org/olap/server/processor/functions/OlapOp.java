package org.olap.server.processor.functions;

import java.util.Collection;
import java.util.List;

import org.olap.server.driver.metadata.ServerMeasure;
import org.olap.server.processor.LevelMemberSet;
import org.olap.server.processor.sql.SetSubquery;
import org.olap.server.processor.sql.TableMapping;
import org.olap4j.OlapException;

public interface OlapOp {

	public abstract List<LevelMemberSet> memberSet( ) throws OlapException ;

	public abstract SetSubquery query(TableMapping mapping, LevelMemberSet layer) throws OlapException;

	public abstract Collection<ServerMeasure> getAllMeasuresUsed() throws OlapException;
	
}
