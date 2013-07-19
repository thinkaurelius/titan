package com.thinkaurelius.titan.diskstorage.solr;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexMutation;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import org.apache.commons.configuration.Configuration;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrSearchIndex implements IndexProvider {

    private Logger log = LoggerFactory.getLogger(SolrSearchIndex.class);

    public static final String SOLR_KEY_FIELD = "solr.key.field.name";
    public static final String SOLR_DEFAULT_KEY_FIELD = "docid";

    private String keyIdField;

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

        keyIdField = config.getString(SOLR_KEY_FIELD, SOLR_DEFAULT_KEY_FIELD);
        log.trace("Will use field name of {} as the identifying field for documents in Solr. This can be changed by updating the {} field in settings.", keyIdField, SOLR_KEY_FIELD);
    }

    /**
     * Unlike the ElasticSearch Index, which is schema free, Solr requires a schema to
     * support searching. This means that you will need to modify the solr schema with the
     * appropriate field definitions in order to work properly.  If you have a running instance
     * of Solr and you modify its schema with new fields, don't forget to re-index!
     * @param store Index store
     * @param key New key to register
     * @param dataType Datatype to register for the key
     * @param tx enclosing transaction
     * @throws StorageException
     */
    @Override
    public void register(String store, String key, Class<?> dataType, TransactionHandle tx) throws StorageException {
        //Since all data types must be defined in the schema.xml, pre-registering a type does not work

    }

    /**
     * Mutates the index (adds and removes fields or entire documents)
     *
     * @param mutations Updates to the index. First map contains all the mutations for each store. The inner map contains
     *                  all changes for each document in an {@link IndexMutation}.
     * @param tx Enclosing transaction
     * @throws StorageException
     * @see IndexMutation
     */
    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, TransactionHandle tx) throws StorageException {
        int bulkRequests = 0;
        try {
            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                String storeName = stores.getKey();

                List<String> deleteIds = new ArrayList<String>();
                Collection<SolrInputDocument> newDocuments = new ArrayList<SolrInputDocument>();
                Collection<SolrInputDocument> updateDocuments = new ArrayList<SolrInputDocument>();

                for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                    String docId = entry.getKey();
                    IndexMutation mutation = entry.getValue();
                    Preconditions.checkArgument(false == (mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(false == mutation.isNew() || false == mutation.hasDeletions());
                    Preconditions.checkArgument(false == mutation.isDeleted() || false == mutation.hasAdditions());

                    //Handle any deletions
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            log.trace("Deleting entire document from Solr {}", docId);
                            deleteIds.add(docId);
                            bulkRequests++;
                        }
                    } else {
                        Set<String> fieldDeletions = Sets.newHashSet(mutation.getDeletions());
                        if (mutation.hasAdditions()) {
                            for (IndexEntry indexEntry : mutation.getAdditions()) {
                                fieldDeletions.remove(indexEntry.key);
                            }
                        }
                        if (false == fieldDeletions.isEmpty()) {
                            Map<String, String> fieldDeletes = new HashMap<String, String>();
                            fieldDeletes.put("set", null);
                            SolrInputDocument doc = new SolrInputDocument();
                            doc.addField(keyIdField, docId);
                            StringBuilder sb = new StringBuilder();
                            for (String fieldToDelete : fieldDeletions) {
                                doc.addField(fieldToDelete, fieldDeletes);
                                sb.append(sb + ",");
                            }
                            log.trace("Deleting individual fields [{}] for document {}", sb.toString(), docId);
                            solrServer.add(doc);

                        }
                    }

                    if (mutation.hasAdditions()) {
                        List<IndexEntry> additions = mutation.getAdditions();
                        if (mutation.isNew()) { //Index
                            log.trace("Adding new document {}", docId);
                            SolrInputDocument newDoc = new SolrInputDocument();
                            for (IndexEntry ie : additions) {
                                newDoc.addField(ie.key, ie.value);
                            }
                            newDocuments.add(newDoc);

                        } else { //Update
                            boolean doUpdate = (false == mutation.hasDeletions());
                            SolrInputDocument updateDoc = new SolrInputDocument();
                            updateDoc.addField(keyIdField, docId);
                            for (IndexEntry ie : additions) {
                                Map<String, String> updateFields = new HashMap<String, String>();
                                updateFields.put("set", ie.value.toString());
                                updateDoc.addField(ie.key, updateFields);
                            }
                            if (doUpdate) {
                                updateDocuments.add(updateDoc);
                            }
                        }

                    }
                }

                if (deleteIds.size() > 0) {
                    solrServer.deleteById(deleteIds);
                }

                if (newDocuments.size() > 0) {
                    solrServer.add(newDocuments);
                }

                if (updateDocuments.size() > 0) {
                    solrServer.add(updateDocuments);
                }
            }
            solrServer.commit();
        } catch (Exception e) {
            throw storageException(e);
        }
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

    private StorageException storageException(Exception solrException) {
        return new TemporaryStorageException("Unable to complete query on Solr.", solrException);
    }
}
