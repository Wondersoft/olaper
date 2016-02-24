package org.olap.server.driver.util;

import java.util.Collections;
import java.util.List;

import org.olap.server.driver.metadata.ServerDimension;
import org.olap.server.processor.LevelMember;
import org.olap4j.CellSetAxis;
import org.olap4j.CellSetAxisMetaData;
import org.olap4j.impl.ArrayNamedListImpl;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.MetadataElement;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Schema;

public class MetadataUtils {


    @SuppressWarnings("serial")
	private static class MetadataElementNamedList<T extends MetadataElement> extends ArrayNamedListImpl<T> {
    	
    	public MetadataElementNamedList(List<T> list){
    		for(T t : list)
    			add(t);
    	}
    	
        public String getName(Object element) {
        	return ((MetadataElement)element).getName();
        }


    }
    
    @SuppressWarnings("serial")
	private static class SchemaNamedList extends ArrayNamedListImpl<Schema> {
    	
    	public SchemaNamedList(List<Schema> list){
    		
    		for(Schema t : list)
    			add(t);
    	}
    	
        public String getName(Object element) {
        	return ((Schema)element).getName();
        }


    }
    
    @SuppressWarnings("serial")
	private static class CatalogNamedList extends ArrayNamedListImpl<Catalog> {
    	
    	public CatalogNamedList(List<Catalog> list){
    		
    		for(Catalog t : list)
    			add(t);
    	}
    	
        public String getName(Object element) {
        	return ((Catalog)element).getName();
        }


    }
    
    @SuppressWarnings("serial")
	private static class CellSetAxisMetaDataNamedList extends ArrayNamedListImpl<CellSetAxisMetaData> {
    	
    	public CellSetAxisMetaDataNamedList(List<CellSetAxis> list){
    		
    		for(CellSetAxis t : list)
    			add(t.getAxisMetaData());
    	}
    	
        public String getName(Object element) {
        	return ((CellSetAxisMetaData)element).getAxisOrdinal().name();
        }


    }

    public static <T extends MetadataElement> NamedList<T> metadataNamedList(List<T> list){
    	return new MetadataElementNamedList<T>(list);
    }
    
    public static  NamedList<CellSetAxisMetaData> cellSetNamedList(List<CellSetAxis> list){
    	return new CellSetAxisMetaDataNamedList(list);
    }

    public static <T extends MetadataElement> NamedList<T> singletoneNamedList(T instance){
    	return new MetadataElementNamedList<T>(Collections.singletonList(instance));
    }
    
    public static NamedList<Schema> singletoneNamedList(Schema instance){
    	return new SchemaNamedList(Collections.singletonList(instance));
    }
    
    public static NamedList<Catalog> catalogNamedList(List<Catalog> list){
    	return new CatalogNamedList(list);
    }
    
    
	public static Member lookupMember(Cube cube, List<IdentifierSegment> segmentList) {
		
		if(segmentList.size() < 3)
			return null;
		
		Dimension dim = cube.getDimensions().get(segmentList.get(0).getName());
		if(dim instanceof ServerDimension){
			
			Hierarchy h = dim.getHierarchies().get(segmentList.get(1).getName());
			if(h!=null){
				
				Level level = null;
				String memberName = null;
				
				if(segmentList.size()==3){		
					level = h.hasAll() ? h.getLevels().get(1) : h.getLevels().get(0);
					memberName = segmentList.get(2).getName();
				}else if(segmentList.size()==4){
					level = h.getLevels().get(segmentList.get(2).getName());
					memberName = segmentList.get(3).getName();
				}
				
				if(level!=null){
					return new LevelMember(level, memberName, memberName,  0);
				}
				
			}
			
		}

		return null;
	}


	public static Level lookupLevel(Cube cube, List<IdentifierSegment> segmentList) {
		
		if(segmentList.size() < 2)
			return null;
		
		Dimension dim = cube.getDimensions().get(segmentList.get(0).getName());
		if(dim instanceof ServerDimension){
			
			Hierarchy h = dim.getHierarchies().get(segmentList.get(1).getName());
			if(h!=null && segmentList.size()==3 && h.getLevels().size()==1){
				return  h.getLevels().get(0);
			}else if(h!=null && segmentList.size()==4){
				return h.getLevels().get(segmentList.get(2).getName());
			}

			
		}

		return null;
	}

}
