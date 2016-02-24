package org.olap.server.driver.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.olap4j.OlapException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ResourceLoader {

	public static <T> T read(String url, Class<T> type) throws OlapException, IOException{
		
		InputStream is = null;
		is = ResourceLoader.class.getClassLoader().getResourceAsStream(url);
		
		if(is==null){
			is = new FileInputStream(url);
		}
		
		try{
			ObjectMapper om = new ObjectMapper();
			return om.readValue(is, type);
		}catch(JsonProcessingException jsonex){
			throw new OlapException("Fail to parse "+url+" due to error: "+jsonex.getMessage());
		}catch(IOException ioex){
			throw new OlapException("Fail to read "+url+" due to error: "+ioex.getMessage());			
		}finally{
			if(is!=null)
				is.close();
		}

		
	}
	
}
