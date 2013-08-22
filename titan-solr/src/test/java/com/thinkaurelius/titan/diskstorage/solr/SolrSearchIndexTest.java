package com.thinkaurelius.titan.diskstorage.solr;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.diskstorage.solr.SolrSearchConstants.*;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrSearchIndexTest extends IndexProviderTest {


    @Override
    public IndexProvider openIndex() throws StorageException {
        return new SolrSearchIndex(getLocalSolrTestConfig());
    }

    public static final Configuration getLocalSolrTestConfig() {
        Configuration config = new BaseConfiguration();
        config.setProperty(SOLR_MODE, SOLR_MODE_HTTP);
        config.setProperty(SOLR_HTTP_URL, "http://localhost:8983/solr");
        config.setProperty(SOLR_HTTP_CONNECTION_TIMEOUT, 10000); //in milliseconds
        config.setProperty(SOLR_CORE_NAMES, "store1,store2,store3");
        config.setProperty(SOLR_KEY_FIELD_NAMES, "store1=document_id,store2=document_id,store3=document_id");
        //config.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir("solr"));
        //String home = "titan-solr/target/test-classes/solr/";
        //config.setProperty(SOLR_HOME, home);


        return config;
    }
}
