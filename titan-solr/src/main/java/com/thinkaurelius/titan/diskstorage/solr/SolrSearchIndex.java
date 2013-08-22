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
import com.thinkaurelius.titan.graphdb.query.keycondition.*;
import org.apache.commons.configuration.Configuration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
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

    private static final int MAX_RESULT_SET_SIZE = 100000;
    private boolean isEmbeddedMode;
    private Logger log = LoggerFactory.getLogger(SolrSearchIndex.class);

    /**
     * Builds a mapping between the core name and its respective Solr Server connection.
     */
    private Map<String, SolrServer> solrServers;
    private Map<String, String> keyFieldIds;

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

        try {
            keyFieldIds = parseKeyFieldsForCores(config);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private Map<String, String> parseKeyFieldsForCores(Configuration config) throws Exception {
        Map<String, String> keyFieldNames = new HashMap<String, String>();
        List<String> coreFieldStatements = config.getList(SOLR_KEY_FIELD_NAMES);
        if (null == coreFieldStatements || coreFieldStatements.size() == 0) {
            log.info("No key field names were defined for any Solr cores. When querying Solr, system will use default key field of: {} to query schema. This can be set using the {} setting and supplying unique id field for that core's schema (e.g. - core1=docid,core2=id,core3=document_id).", SOLR_DEFAULT_KEY_FIELD, SOLR_KEY_FIELD_NAMES);
            for (String coreName : coreNames) {
                keyFieldNames.put(coreName, SOLR_DEFAULT_KEY_FIELD);
            }
        } else {
            for (String coreFieldStatement : coreFieldStatements) {
                String[] parts = coreFieldStatement.trim().split("=");
                if (parts == null || parts.length == 0) {
                    throw new Exception("Unable to parse the core name/ key field name pair. It should be of the format core=field");
                }
                String coreName = parts[0];
                String keyFieldName = parts[1];
                keyFieldNames.put(coreName, keyFieldName);
                log.trace("Will use field name of {} as the identifying field for Solr core {}. This can be changed by updating the {} field in settings.",
                        keyFieldName, coreName, SOLR_KEY_FIELD_NAMES);
            }
        }
        return keyFieldNames;
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
                String keyIdField = keyFieldIds.get(coreName);

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
        List<String> result = null;
        String core = query.getStore();
        String keyIdField = keyFieldIds.get(core);
        SolrServer solr = this.solrServers.get(core);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("*:*");
        solrQuery = buildQuery(solrQuery, query.getCondition());
        if (query.hasLimit()) {
            solrQuery.setRows(query.getLimit());
        } else {
            solrQuery.setRows(MAX_RESULT_SET_SIZE);
        }
        try {
            QueryResponse response = solr.query(solrQuery);
            log.debug("Executed query [{}] in {} ms", query.getCondition(), response.getElapsedTime());
            int totalHits = response.getResults().size();
            if (false == query.hasLimit() && totalHits >= MAX_RESULT_SET_SIZE) {
                log.warn("Query result set truncated to first [{}] elements for query: {}", MAX_RESULT_SET_SIZE, query);
            }
            result = new ArrayList<String>(totalHits);
            for (SolrDocument hit : response.getResults()) {
                result.add(hit.getFieldValue(keyIdField).toString());
            }

        } catch (SolrServerException e) {
            log.error("Unable to query Solr index.", e);
        }

        return result;
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
                    q.addFacetQuery(key + ":[" + i.getStart() + " TO " + i.getEnd() + "]");
                    return q;
                } else {
                    Preconditions.checkArgument(value instanceof Number);

                    switch (numRel) {
                        case EQUAL:
                            q.addFilterQuery(key + ":" + value.toString());
                            return q;
                        case NOT_EQUAL:
                            q.addFilterQuery("-" + key + ":" + value.toString());
                            return q;
                        case LESS_THAN:
                            //use right curly to mean up to but not including value
                            q.addFilterQuery(key + ":[* TO " + value.toString() + "}");
                            return q;
                        case LESS_THAN_EQUAL:
                            q.addFilterQuery(key + ":[* TO " + value.toString() + "]");
                            return q;
                        case GREATER_THAN:
                            //use left curly to mean greater than but not including value
                            q.addFilterQuery(key + ":{" + value.toString() + " TO *]");
                            return q;
                        case GREATER_THAN_EQUAL:
                            q.addFilterQuery(key + ":[" + value.toString() + " TO *]");
                            return q;
                        default: throw new IllegalArgumentException("Unexpected relation: " + numRel);
                    }
                }
            } else if (value instanceof String) {
                if (relation == Text.CONTAINS) {
                    //e.g. - if terms tomorrow and world were supplied, and fq=text:(tomorrow  world)
                    //sample data set would return 2 documents: one where text = Tomorrow is the World,
                    //and the second where text = Hello World
                    q.addFilterQuery(key + ":("+((String) value).toLowerCase()+")");
                    return q;
                } else {
                    throw new IllegalArgumentException("Relation is not supported for string value: " + relation);
                }
            } else if (value instanceof Geoshape) {
                Geoshape geo = (Geoshape)value;
                if (geo.getType() == Geoshape.Type.CIRCLE) {
                    Geoshape.Point center = geo.getPoint();
                    q.addFilterQuery("{!geofilt sfield=" + key +
                            " pt=" + center.getLongitude() + "," + center.getLatitude() +
                            " d=" + geo.getRadius() + "}"); //distance in kilometers
                    return q;
                } else if (geo.getType() == Geoshape.Type.BOX) {
                    Geoshape.Point southwest = geo.getPoint(0);
                    Geoshape.Point northeast = geo.getPoint(1);
                    q.addFilterQuery(key + ":[" + southwest.getLongitude() + "," + southwest.getLatitude() +
                                    " TO " + northeast.getLongitude() + "," + northeast.getLatitude() + "]");
                    return q;
                } else if (geo.getType() == Geoshape.Type.POLYGON) {
                    List<Geoshape.Point> coordinates = getPolygonPoints(geo);
                    StringBuilder poly = new StringBuilder(key + ":\"IsWithin(POLYGON((");
                    for (Geoshape.Point coordinate : coordinates) {
                        poly.append(coordinate.getLongitude() + " " + coordinate.getLatitude() + ", ");
                    }
                    //close the polygon with the first coordinate
                    poly.append(coordinates.get(0).getLongitude() + " " + coordinates.get(0).getLatitude());
                    poly.append(")))\" distErrPct=0");
                    q.addFilterQuery(poly.toString());
                    return q;
                }
            }
        } else if (condition instanceof KeyNot) {
            String[] filterConditions = q.getFilterQueries();
           for (String filterCondition : filterConditions) {
                //if (filterCondition.contains(key)) {
                    q.removeFilterQuery(filterCondition);
                    q.addFilterQuery("-" + filterCondition);
                //}
            }
            return q;
        } else if (condition instanceof KeyAnd) {
            for (KeyCondition<String> c : condition.getChildren()) {
                buildQuery(q, c);
            }
            return q;
        } else {
            throw new IllegalArgumentException("Invalid condition: " + condition);
        }
        return null;
    }

    private List<Geoshape.Point> getPolygonPoints(Geoshape polygon) {
        List<Geoshape.Point> locations = new ArrayList<Geoshape.Point>();

        int index = 0;
        boolean hasCoordinates = true;
        while (hasCoordinates) {
            try {
                locations.add(polygon.getPoint(index));
            } catch (ArrayIndexOutOfBoundsException ignore) {
                //just means we asked for a point past the size of the list
                //of known coordinates
                hasCoordinates = false;
            }
        }

        return locations;
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
