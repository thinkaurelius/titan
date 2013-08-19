package com.thinkaurelius.titan.diskstorage.solr;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexMutation;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.solr.transform.GeoToWktConverter;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAtom;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyCondition;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;
import org.apache.commons.configuration.Configuration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.thinkaurelius.titan.diskstorage.solr.SolrSearchConstants.*;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrSearchIndex implements IndexProvider {

    private boolean isEmbeddedMode;
    private Logger log = LoggerFactory.getLogger(SolrSearchIndex.class);

    private String keyIdField;

    /**
     * Builds a mapping between the core name and its respective Solr Server connection.
     */
    private Map<String, SolrServer> solrServers;

    private List<String> coreNames;

    public SolrSearchIndex(Configuration config) {
        //There are several different modes in which solr can be found running:
        //1. EmbeddedSolrServer - used when Solr runs in same JVM as titan. Good for development but not encouraged
        //2. HttpSolrServer - used to connect to Solr instance via Apache HTTP client to a specific solr instance bound to a specific URL.
        //3. CloudSolrServer - used to connect to a SolrCloud cluster that uses Apache Zookeeper. This lets clients hit one host and Zookeeper distributes queries and writes automatically
        SolrServerFactory factory = new SolrServerFactory();

        coreNames = SolrSearchUtils.parseConfigForCoreNames(config);

        try {
            solrServers = factory.buildSolrServers(config);

            String mode = config.getString(SOLR_MODE, SOLR_MODE_EMBEDDED);
            if (mode.equalsIgnoreCase(SOLR_MODE_EMBEDDED)) {
                isEmbeddedMode = true;
            }

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

        try {
            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                String coreName = stores.getKey();
                SolrServer solr = solrServers.get(coreName);

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
                            solr.add(doc);
                            solr.commit();

                        }
                    }

                    if (mutation.hasAdditions()) {
                        List<IndexEntry> additions = mutation.getAdditions();
                        if (mutation.isNew()) { //Index
                            log.trace("Adding new document {}", docId);
                            SolrInputDocument newDoc = new SolrInputDocument();
                            newDoc.addField(keyIdField, docId);
                            for (IndexEntry ie : additions) {
                                Object fieldValue = ie.value;
                                if (GeoToWktConverter.isGeoshape(ie.value))  {
                                    fieldValue = GeoToWktConverter.convertToWktString(ie.value);
                                }
                                newDoc.addField(ie.key, fieldValue);
                            }
                            //newDocuments.add(newDoc);
                            solr.add(newDoc);

                            solr.commit();

                        } else { //Update
                            boolean doUpdate = (false == mutation.hasDeletions());
                            SolrInputDocument updateDoc = new SolrInputDocument();
                            updateDoc.addField(keyIdField, docId);
                            for (IndexEntry ie : additions) {
                                Map<String, String> updateFields = new HashMap<String, String>();
                                Object fieldValue = ie.value;
                                if (GeoToWktConverter.isGeoshape(ie.value))  {
                                    fieldValue = GeoToWktConverter.convertToWktString(ie.value);
                                }
                                updateFields.put("set", fieldValue.toString());
                                updateDoc.addField(ie.key, updateFields);
                            }
                            if (doUpdate) {
                                //updateDocuments.add(updateDoc);
                                solr.add(updateDoc);
                                solr.commit();
                            }
                        }

                    }
                }

//                commitDeletes(server, deleteIds);
//                commitDocumentChanges(server, newDocuments);
//                commitDocumentChanges(server, updateDocuments);
            }

        } catch (Exception e) {
            throw storageException(e);
        }
    }

    private void commitDocumentChanges(SolrServer server, Collection<SolrInputDocument> documents) throws SolrServerException, IOException {
        if (documents.size() > 0) {
            if (isEmbeddedMode) {
                int commitWithinMs = 10000;
                for(SolrInputDocument doc : documents) {
                    server.add(doc, commitWithinMs);
                    server.commit();
                }
            } else {
                server.add(documents);
                server.commit();
            }
        }
    }

    private void commitDeletes(SolrServer server, List<String> deleteIds) throws SolrServerException, IOException {
        if (deleteIds.size() > 0) {
            server.deleteById(deleteIds);
            server.commit();
        }
    }


    @Override
    public List<String> query(IndexQuery query, TransactionHandle tx) throws StorageException {
        String core = query.getStore();
        SolrServer solr = this.solrServers.get(core);
        SolrQuery solrQuery = new SolrQuery();


        return new ArrayList<String>();
    }

    public SolrQuery buildQuery(SolrQuery q, KeyCondition<String> condition) {
        if (condition instanceof KeyAtom) {
            KeyAtom<String> atom= (KeyAtom<String>) condition;
            Object value = atom.getCondition();
            String key = atom.getKey();
            Relation relation = atom.getRelation();

            if (value instanceof Number ||
                value instanceof Interval) {

                Preconditions.checkArgument(relation instanceof Cmp, "Relation not supported on numeric types: " + relation);
                Cmp numRel = (Cmp) relation;
                if (numRel == Cmp.INTERVAL) {
                    Interval i = (Interval)value;
                    q.addFacetQuery(key + ":[" + i.getStart() + " TO " + "]");
                    return q;
                } else {
                    Preconditions.checkArgument(value instanceof Number);

                    switch (numRel) {
                        case EQUAL:
                            q.addFacetQuery(key + ":" + value.toString());
                            return q;
                        case NOT_EQUAL:
                            q.addFacetQuery("-" + key + ":" + value.toString());
                            return q;
                        case LESS_THAN:
                            //use right curly to mean up to but not including value
                            q.addFacetQuery(key + ":[* TO " + value.toString() + "}");
                            return q;
                        case LESS_THAN_EQUAL:
                            q.addFacetQuery(key + ":[* TO " + value.toString() + "]");
                            return q;
                        case GREATER_THAN:
                            //use left curly to mean greater than but not including value
                            q.addFacetQuery(key + ":{" + value.toString() + " TO *]");
                            return q;
                        case GREATER_THAN_EQUAL:
                            q.addFacetQuery(key + ":[" + value.toString() + " TO *]");
                            return q;
                        default: throw new IllegalArgumentException("Unexpected relation: " + numRel);
                    }
                }
            } else if (value instanceof String) {
                if (relation == Text.CONTAINS) {

                }
            }
        }
        return null;
    }

    /**
     * Solr handles all transactions on the server-side. That means all
     * commit, optimize, or rollback applies since the last commit/optimize/rollback.
     * Solr documentation recommends best way to update Solr is in one process to avoid
     * race conditions.
     * @return
     * @throws StorageException
     */
    @Override
    public TransactionHandle beginTransaction() throws StorageException {
        return TransactionHandle.NO_TRANSACTION;
    }

    @Override
    public void close() throws StorageException {
        for (Map.Entry<String, SolrServer> pair : solrServers.entrySet()) {

            String coreName = pair.getKey();
            SolrServer server = pair.getValue();
            log.trace("Shutting down connection to Solr Core: " + coreName);
            server.shutdown();

            if (isEmbeddedMode) {
                break;
            }


        }
        solrServers.clear();
    }

    @Override
    public void clearStorage() throws StorageException {
        try {
            for (Map.Entry<String, SolrServer> pair : solrServers.entrySet()) {
                String coreName = pair.getKey();
                SolrServer server = pair.getValue();
                log.trace("Clearing storage from Solr Core: " + coreName);
                server.deleteByQuery("*:*");
            }
        } catch (SolrServerException e) {
            log.error("Unable to clear storage from index due to server error on Solr.", e);
        } catch (IOException e) {
            log.error("Unable to clear storage from index due to low-level I/O error.", e);
        } catch (Exception e) {
            log.error("Unable to clear storage from index due to general error.", e);
        }
    }

    @Override
    public boolean supports(Class<?> dataType, Relation relation) {
        if (Number.class.isAssignableFrom(dataType)) {
            if (relation instanceof Cmp) {
                return true;
            } else {
                return false;
            }
        } else if (dataType == Geoshape.class) {
            return relation == Geo.WITHIN;
        } else if (dataType == String.class) {
            return relation == Text.CONTAINS;
        } else {
            return false;
        }
    }

    @Override
    public boolean supports(Class<?> dataType) {
        if (Number.class.isAssignableFrom(dataType) ||
                dataType == Geoshape.class ||
                dataType == String.class) {
            return true;
        }
        return false;
    }

    private StorageException storageException(Exception solrException) {
        return new TemporaryStorageException("Unable to complete query on Solr.", solrException);
    }
}
