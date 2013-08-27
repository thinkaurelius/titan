package com.thinkaurelius.titan.diskstorage.solr;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAtom;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.diskstorage.solr.SolrSearchConstants.*;
import static org.junit.Assert.assertEquals;
import  org.junit.Test;

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

        //SOLR_MODE_HTTP
        /*
        config.setProperty(SOLR_MODE, SOLR_MODE_HTTP);
        config.setProperty(SOLR_HTTP_URL, "http://localhost:8983/solr");
        config.setProperty(SOLR_HTTP_CONNECTION_TIMEOUT, 10000); //in milliseconds
        */

        //SOLR_MODE_EMBEDDED
        config.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir("solr"));
        String home = "titan-solr/target/test-classes/solr/";
        config.setProperty(SOLR_HOME, home);



        config.setProperty(SOLR_CORE_NAMES, "store,store1,store2,store3");
        config.setProperty(SOLR_KEY_FIELD_NAMES, "store=document_id,store1=document_id,store2=document_id,store3=document_id");




        return config;
    }

    @Test
    public void storeWithBoundingBoxGeospatialSearch() throws StorageException
    {
        this.openIndex().clearStorage();;

        String[] stores = new String[] { "store1" };

        Map<String,Object> doc1 = getDocument("Hello world",1001,5.2, Geoshape.point(48.0, 0.0));
        Map<String,Object> doc2 = getDocument("Tomorrow is the world",1010,8.5,Geoshape.point(49.0,1.0));
        Map<String,Object> doc3 = getDocument("Hello Bob, are you there?", -500, 10.1, Geoshape.point(47.0, 10.0));

        for (String store : stores) {
            initialize(store);

            add(store,"doc1",doc1,true);
            add(store,"doc2",doc2,true);
            add(store,"doc3",doc3,false);
        }

        clopen();

        for (String store : stores) {

            List<String> result = tx.query(new IndexQuery(store, KeyAtom.of("location", Geo.WITHIN, Geoshape.box(46.5, -0.5, 50.5, 10.5))));
            assertEquals(3,result.size());
            assertEquals(ImmutableSet.of("doc1", "doc2", "doc3"), ImmutableSet.copyOf(result));
        }
    }
}
