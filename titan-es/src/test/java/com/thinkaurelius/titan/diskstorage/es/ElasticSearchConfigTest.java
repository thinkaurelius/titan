package com.thinkaurelius.titan.diskstorage.es;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NS;

public class ElasticSearchConfigTest {

    @Test
    public void elasticSearchShouldStartInLocalModeWhenLocalConfigrationMandates() throws Exception {
        CommonsConfiguration localConfig = new CommonsConfiguration();

        localConfig.set("storage.backend", "inmemory");
        localConfig.set("storage.keyspace", "9a077df8-e045-456e-b5b6-d918b4a11a95");
        localConfig.set("storage.directory", "/tmp/inmemory");

        localConfig.set("index.search.index-name", "542634bb-2b57-4ac6-b3de-11665d894297");
        localConfig.set("index.search.backend", "elasticsearch");
        localConfig.set("index.search.hostname", "localhost");
        localConfig.set("index.search.directory", "/tmp/elasticsearch");

        localConfig.set("index.search.elasticsearch.client-only", "false");
        localConfig.set("index.search.elasticsearch.local-mode", "true");
        localConfig.set("index.search.elasticsearch.sniff", "false");
        localConfig.set("index.search.elasticsearch.cluster-name", "foo");

        ElasticSearchIndex index = null;

        try {
            Configuration config = new GraphDatabaseConfiguration(localConfig).getConfiguration();
            for(String configuredIndex: config.getContainedNamespaces(INDEX_NS)) {
                index = new ElasticSearchIndex(config.restrictTo(configuredIndex));
            }
            assertNotNull("Could not instantiate Elastic Search", index);
        } catch(Exception e) {
            throw e;
        }
    }
}
