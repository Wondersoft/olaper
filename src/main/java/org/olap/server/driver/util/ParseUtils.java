package org.olap.server.driver.util;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.olap4j.mdx.CallNode;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.NameSegment;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.ParseTreeWriter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.WithMemberNode;
import org.olap4j.mdx.WithSetNode;
import org.olap4j.mdx.parser.MdxParser;

public class ParseUtils {

	public static String[] identifierNames(IdentifierNode inode){
		
		String[] ids = new String[inode.getSegmentList().size()];
		
		for(int i=0; i<inode.getSegmentList().size(); i++){
			NameSegment seg = (NameSegment) inode.getSegmentList().get(i);
			ids[i] = seg.getName();
		}
		
		return ids;
	}
	
    public static String toString(ParseTreeNode node) {
        StringWriter sw = new StringWriter();
        ParseTreeWriter parseTreeWriter = new ParseTreeWriter(sw);
        node.unparse(parseTreeWriter);
        return sw.toString();
    }
	
    public static SelectNode normalize(MdxParser parser, SelectNode selectNode){
    	
		SelectNode acopy = selectNode.deepCopy();
		acopy.getWithList().clear();
		String resolvedMdx = ParseUtils.toString(acopy);
		
		for(ParseTreeNode wnode: selectNode.getWithList()){
			if(wnode instanceof WithSetNode){
				WithSetNode wsn = (WithSetNode) wnode;
				String exprMdx = ParseUtils.toString(wsn.getExpression());
				String nameMdx = wsn.getIdentifier().toString();
				resolvedMdx = resolvedMdx.replace(nameMdx, exprMdx);				
			}else if(wnode instanceof WithMemberNode){
				WithMemberNode wsn = (WithMemberNode) wnode;
				String exprMdx = ParseUtils.toString(wsn.getExpression());
				String nameMdx = wsn.getIdentifier().toString();
				resolvedMdx = resolvedMdx.replace(nameMdx, exprMdx);								
				
			}
		}
		
		
		
		return parser.parseSelect(resolvedMdx);
    }
    
    public static List<IdentifierNode> getAllIdentifiers(CallNode callNode){
    	return getAllIdentifiers(callNode, new ArrayList<IdentifierNode>());
    }
    
    private static List<IdentifierNode> getAllIdentifiers(CallNode callNode, List<IdentifierNode> list){
    	
    	for(ParseTreeNode node : callNode.getArgList()){
    		
    		if(node instanceof CallNode){
    			getAllIdentifiers((CallNode) node, list);
    		}else if(node instanceof IdentifierNode){
    			if(!list.contains(node))
    				list.add((IdentifierNode) node);
    		}
    		
    	}
    	
    	return list;
    }
    
}
