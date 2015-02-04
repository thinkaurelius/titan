package com.thinkaurelius.titan.graphdb.mapdb;

import com.thinkaurelius.titan.MapDBStorageSetup;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;

import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.CLIENT_ONLY;
import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.LOCAL_MODE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class MapDBElasticsearchTest extends TitanIndexTest {

    public MapDBElasticsearchTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = MapDBStorageSetup.getMapDBConfiguration();
        //Add index
        config.set(INDEX_BACKEND,"elasticsearch",INDEX);
        config.set(LOCAL_MODE,true,INDEX);
        config.set(CLIENT_ONLY,false,INDEX);
        config.set(INDEX_DIRECTORY,StorageSetup.getHomeDir("es"),INDEX);
        return config.getConfiguration();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }
}