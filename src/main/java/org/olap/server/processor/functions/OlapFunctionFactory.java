package org.olap.server.processor.functions;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.olap.server.driver.util.ParseUtils;
import org.olap4j.OlapException;
import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.Quoting;
import org.olap4j.metadata.Cube;
import org.reflections.Reflections;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class OlapFunctionFactory {
	

	
	private Map<String,  Class> functionMap = new HashMap<String, Class>();
	
	public static final OlapFunctionFactory INSTANCE = new OlapFunctionFactory( 
				annotated_classes(OlapFunction.class.getPackage(), OlapFunctionName.class)		
	);
			
	public OlapFunctionFactory(Set<Class<?>> f_classes) {

		for(Class c: f_classes){
			OlapFunctionName name = (OlapFunctionName) c.getAnnotation(OlapFunctionName.class);
			functionMap.put(name.name(), c);
		}
	}


	private static Set<Class<?>> annotated_classes(Package in_package, Class<OlapFunctionName> with_annotation) {
		
		Reflections reflections = new Reflections(in_package.getName());
		
		return  reflections.getTypesAnnotatedWith(with_annotation);
		
	}


	public OlapOp function(ParseTreeNode node, Cube cube) throws OlapException{
		
		if(node==null){
			return new NullFunction(cube);
		}else if(node instanceof CallNode ){
			
			String name = ((CallNode) node).getOperatorName().toLowerCase();
			OlapOp op = getOlapOp(node, name, cube);	
			if(op==null)
				throw new OlapException("Function "+name+" is not implemented, fail to process "+ParseUtils.toString(node));
			return op;
			
		}else if(node instanceof IdentifierNode){
			
			IdentifierNode inode = (IdentifierNode) node;
			IdentifierSegment seg_op = inode.getSegmentList().get(inode.getSegmentList().size()-1);
			
			OlapOp op = null;
			
			if(seg_op.getQuoting()!=Quoting.QUOTED)
				op = getOlapOp(node, seg_op.getName(), cube);
			
			if(op!=null)
				return op;
			
			return new IdentifiedMember((IdentifierNode) node, cube);
			
		}else{
			throw new OlapException("Unsupported expression: "+ParseUtils.toString(node));
		}
		
	}


	protected OlapOp getOlapOp(ParseTreeNode op_node, String name, Cube cube)
			throws OlapException {
		
		Class c = functionMap.get(name.toLowerCase());
		if(c==null)
			return null;
		try {
			Constructor cons = c.getConstructor(new Class[]{op_node.getClass(), Cube.class});
			return (OlapOp) cons.newInstance(op_node, cube);
		} catch (Exception e) {
			throw new RuntimeException("Fail to run function "+name, e);
		}
		
	}

	
	
	
}
