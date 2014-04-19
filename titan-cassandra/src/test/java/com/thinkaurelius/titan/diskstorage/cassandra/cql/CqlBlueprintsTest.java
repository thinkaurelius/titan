package com.thinkaurelius.titan.diskstorage.cassandra.cql;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NAMESPACE;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.blueprints.TitanBlueprintsTest;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.tinkerpop.blueprints.Graph;

public class CqlBlueprintsTest extends TitanBlueprintsTest {

	@Override
	public Graph generateGraph() {
		Configuration config = new BaseConfiguration();
		config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "cassandracql");
		return TitanFactory.open(config);
	}

	@Override
	public boolean supportsMultipleGraphs() {
		return false;
	}

	@Override
	public void startUp() {

	}

	@Override
	public void shutDown() {

	}

	@Override
	public void cleanUp() throws StorageException {
		Configuration config = new BaseConfiguration();
		config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "cassandra-cql");
		CqlStoreManager manager = new CqlStoreManager(config);
		manager.clearStorage();
	}

	@Override
	public Graph generateGraph(String graph) {
		throw new UnsupportedOperationException();
	}
	
	 
}
