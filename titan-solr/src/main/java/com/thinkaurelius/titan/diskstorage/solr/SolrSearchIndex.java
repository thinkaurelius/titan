package com.thinkaurelius.titan.diskstorage.solr;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.indexing.IndexMutation;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import org.apache.commons.configuration.Configuration;
import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrSearchIndex implements IndexProvider {

    private Logger log = LoggerFactory.getLogger(SolrSearchIndex.class);

    private SolrServer solrServer;

    public SolrSearchIndex(Configuration config) {
        //There are several different modes in which solr can be found running:
        //1. EmbeddedSolrServer - used when Solr runs in same JVM as titan. Good for development but not encouraged
        //2. HttpSolrServer - used to connect to Solr instance via Apache HTTP client to a specific solr instance bound to a specific URL.
        //3. CloudSolrServer - used to connect to a SolrCloud cluster that uses Apache Zookeeper. This lets clients hit one host and Zookeeper distributes queries and writes automatically
        SolrServerFactory factory = new SolrServerFactory();
        try {
            solrServer = factory.buildSolrServer(config);
        } catch (Exception e) {
            log.error("Unable to generate a Solr Server connection.", e);
        }
    }

    @Override
    public void register(String store, String key, Class<?> dataType, TransactionHandle tx) throws StorageException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, TransactionHandle tx) throws StorageException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<String> query(IndexQuery query, TransactionHandle tx) throws StorageException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TransactionHandle beginTransaction() throws StorageException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws StorageException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearStorage() throws StorageException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supports(Class<?> dataType, Relation relation) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supports(Class<?> dataType) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
