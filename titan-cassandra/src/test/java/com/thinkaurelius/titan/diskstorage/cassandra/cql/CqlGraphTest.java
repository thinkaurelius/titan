package com.thinkaurelius.titan.diskstorage.cassandra.cql;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NAMESPACE;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class CqlGraphTest extends TitanGraphTest {

	public CqlGraphTest() {
		
		super(getConfig());
	}

	private static  Configuration getConfig() {
		Configuration config = new BaseConfiguration();
		config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "cassandracql");
		return config;
		
	}
	
	

}
