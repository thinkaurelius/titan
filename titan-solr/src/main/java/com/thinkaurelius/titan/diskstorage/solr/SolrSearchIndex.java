package com.thinkaurelius.titan.diskstorage.solr;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.indexing.IndexMutation;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrSearchIndex implements IndexProvider {

    private Logger log = LoggerFactory.getLogger(SolrSearchIndex.class);

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
