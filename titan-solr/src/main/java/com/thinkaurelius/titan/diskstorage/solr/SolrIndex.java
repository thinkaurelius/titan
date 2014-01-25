package com.thinkaurelius.titan.diskstorage.solr;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.spatial4j.core.exception.InvalidShapeException;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.diskstorage.solr.transform.GeoToWktConverter;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.Not;
import com.thinkaurelius.titan.graphdb.query.condition.PredicateCondition;
import org.apache.commons.configuration.Configuration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.thinkaurelius.titan.diskstorage.solr.SolrConstants.*;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrIndex implements IndexProvider {

    private static final int MAX_RESULT_SET_SIZE = 100000;
    private static int BATCH_SIZE;
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private boolean isEmbeddedMode;
    private Logger log = LoggerFactory.getLogger(SolrIndex.class);

    /**
     * Builds a mapping between the core name and its respective Solr Server connection.
     */
    private Map<String, SolrServer> solrServers;
    private Map<String, String> keyFieldIds;

    private List<String> coreNames;

    /**
     *  There are several different modes in which the index can be configured with Solr:
     *  <ol>
     *    <li>EmbeddedSolrServer - used when Solr runs in same JVM as titan. Good for development but not encouraged</li>
     *    <li>HttpSolrServer - used to connect to Solr instance via Apache HTTP client to a specific solr instance bound to a specific URL.</li>
     *    <li>CloudSolrServer - used to connect to a SolrCloud cluster that uses Apache Zookeeper. This lets clients hit one host and Zookeeper distributes queries and writes automatically</li>
     *  </ol>
     *  <p>
     *      An example follows in configuring Solr support for Titan::
     *      <pre>
     *          {@code
     *              import org.apache.commons.configuration.Configuration;
     *              import static com.thinkaurelius.titan.diskstorage.solr.SolrSearchConstants.*;
     *              import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
     *
     *              public class MyClass {
     *                  private Configuration config;
     *
     *                  public MyClass(String mode) {
     *                      config = new BaseConfiguration()
     *                      if (mode.equals(SOLR_MODE_EMBEDDED)) {
     *                          config.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir("solr"));
     *                          String home = "titan-solr/target/test-classes/solr";
     *                          config.setProperty(SOLR_MODE, SOLR_MODE_EMBEDDED);
     *                          config.setProperty(SOLR_CORE_NAMES, "core1,core2,core3");
     *                          config.setProperty(SOLR_HOME, home);
     *
     *                      } else if (mode.equals(SOLR_MODE_HTTP)) {
     *                          config.setProperty(SOLR_MODE, SOLR_MODE_HTTP);
     *                          config.setProperty(SOLR_HTTP_URL, "http://localhost:8983/solr");
     *                          config.setProperty(SOLR_HTTP_CONNECTION_TIMEOUT, 10000); //in milliseconds
     *
     *                      } else if (mode.equals(SOLR_MODE_CLOUD)) {
     *                          config.setProperty(SOLR_MODE, SOLR_MODE_CLOUD);
     *                          //Don't add the protocol: http:// or https:// to the url
     *                          config.setProperty(SOLR_CLOUD_ZOOKEEPER_URL, "localhost:2181")
     *                          //Set the default collection for Solr in Zookeeper.
     *                          //Titan allows for more but just needs a default one as a fallback
     *                          config.setProperty(SOLR_CLOUD_COLLECTION, "store");
     *                      }
     *
     *                      config.setProperty(SOLR_CORE_NAMES, "store,store1,store2,store3");
     *                      //A key/value list where key is the core name and value us the name of the field used in solr to uniquely identify a document.
     *                      config.setProperty(SOLR_KEY_FIELD_NAMES, "store=document_id,store1=document_id,store2=document_id,store3=document_id");
     *                  }
     *              }
     *          }
     *      </pre>
     *  </p>
     *  <p>
     *      Something to keep in mind when using Solr as the {@link com.thinkaurelius.titan.diskstorage.indexing.IndexProvider} for Titan. Solr has many different
     *      types of indexes for backing your field types defined in the schema. Whenever you use a solr.Textfield type, string values are split up into individual
     *      tokens. This is usually desirable except in cases where you are searching for a phrase that begins with a specified prefix as in
     *      the {@link com.thinkaurelius.titan.core.attribute.Text#PREFIX} enumeration that can be used in gremlin searches. In that case, the SolrIndex will use the
     *      convention of assuming you have defined a field of the same name as the solr.Textfield but will be of type solr.Strfield.
     *  </p>
     *  <p>
     *      For example, let's say you have two documents in Solr with a field called description. One document has a description of "Tomorrow is the world", the other, "World domination".
     *      If you defined the description field in your schema and set it to type solr.TextField a PREFIX based search like the one below would return both documents:
     *      <pre>
     *          {@code
     *          g.query().has("description",Text.PREFIX,"World")
     *          }
     *      </pre>
     *  </p>
     *  <p>
     *      However, if you create a copyField with the name "descriptionString" and set its type to solr.StrField, the PREFIX search defined above would behave as expected
     *      and only return the document with description "World domination" as its a raw string that is not tokenized in the index.
     *  </p>
     * @param config Titan configuration passed in at start up time
     */
    public SolrIndex(Configuration config) {

        SolrServerFactory factory = new SolrServerFactory();
        coreNames = SolrUtils.parseConfigForCoreNames(config);

        try {
            solrServers = factory.buildSolrServers(config);
            detectAndSetEmbeddedMode(config);
        } catch (Exception e) {
            log.error("Unable to generate a Solr Server connection.", e);
        }

        try {
            keyFieldIds = parseKeyFieldsForCores(config);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        BATCH_SIZE =  config.getInt(SOLR_COMMIT_BATCH_SIZE, DEFAULT_BATCH_SIZE);
    }

    private void detectAndSetEmbeddedMode(Configuration config) {
        String mode = config.getString(SOLR_MODE, SOLR_MODE_HTTP);
        if (mode.equalsIgnoreCase(SOLR_MODE_EMBEDDED)) {
            isEmbeddedMode = true;
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
     * @param information Datatype to register for the key
     * @param tx enclosing transaction
     * @throws StorageException
     */
    @Override
    public void register(String store, String key, KeyInformation information, TransactionHandle tx) throws StorageException {
        //Since all data types must be defined in the schema.xml, pre-registering a type does not work
    }

    /**
     * Mutates the index (adds and removes fields or entire documents)
     *
     * @param mutations Updates to the index. First map contains all the mutations for each store. The inner map contains
     *                  all changes for each document in an {@link IndexMutation}.
     * @param informations
     * @param tx Enclosing transaction
     * @throws StorageException
     * @see IndexMutation
     */
    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, TransactionHandle tx) throws StorageException {
        //TODO: research usage of the informations parameter
        try {
            List<String> deleteIds = new ArrayList<String>();
            Collection<SolrInputDocument> newDocuments = new ArrayList<SolrInputDocument>();
            Collection<SolrInputDocument> updateDocuments = new ArrayList<SolrInputDocument>();
            boolean isLastBatch = false;

            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                String coreName = stores.getKey();
                SolrServer solr = solrServers.get(coreName);
                if (null == solr) {
                    throw new Exception("Store name of " + coreName +  " provided in the query does not exist in Solr or has not been specified in the configuration of titan.");
                }
                String keyIdField = keyFieldIds.get(coreName);
                int numProcessed = 0;

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
                        } else {
                            deleteIndividualFieldsFromIndex(solr, keyIdField, docId, mutation);
                        }
                    } else {
                        Set<String> fieldDeletions = Sets.newHashSet(mutation.getDeletions());
                        if (mutation.hasAdditions()) {
                            for (IndexEntry indexEntry : mutation.getAdditions()) {
                                fieldDeletions.remove(indexEntry.key);
                            }
                        }
                        deleteIndividualFieldsFromIndex(solr, keyIdField, docId, mutation);
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
                            newDocuments.add(newDoc);

                        } else { //Update
                            //boolean doUpdate = (false == mutation.hasDeletions());
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

                            updateDocuments.add(updateDoc);
                        }
                    }
                    numProcessed++;
                    if (numProcessed == stores.getValue().size()) {
                        isLastBatch = true;
                    }

                    commitDeletes(solr, deleteIds, isLastBatch);
                    commitDocumentChanges(solr, newDocuments, isLastBatch);
                    commitDocumentChanges(solr, updateDocuments, isLastBatch);
                }
            }
        } catch (Exception e) {
            throw storageException(e);
        }
    }

    private void deleteIndividualFieldsFromIndex(SolrServer solr, String keyIdField, String docId, IndexMutation mutation) throws SolrServerException, IOException {
        Set<String> fieldDeletions = Sets.newHashSet(mutation.getDeletions());
        if (false == fieldDeletions.isEmpty()) {
            Map<String, String> fieldDeletes = new HashMap<String, String>();
            fieldDeletes.put("set", null);
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(keyIdField, docId);
            StringBuilder sb = new StringBuilder();
            for (String fieldToDelete : fieldDeletions) {
                doc.addField(fieldToDelete, fieldDeletes);
                sb.append(fieldToDelete + ",");
            }
            log.trace("Deleting individual fields [{}] for document {}", sb.toString(), docId);
            solr.add(doc);
            solr.commit();

        }
    }

    private void commitDocumentChanges(SolrServer server, Collection<SolrInputDocument> documents, boolean isLastBatch) throws SolrServerException, IOException {
        int numUpdates = documents.size();
        if (numUpdates == 0) {
            return;
        }

        try {
            if (numUpdates >= BATCH_SIZE || isLastBatch) {
                if (isEmbeddedMode) {
                    int commitWithinMs = 10000;
                    for(SolrInputDocument doc : documents) {
                        server.add(doc, commitWithinMs);
                        server.commit();
                    }
                } else {

                    server.add(documents);
                    server.commit();
                    documents.clear();
                }
            }
        } catch (HttpSolrServer.RemoteSolrException rse) {
            log.error("Unable to save documents to Solr as one of the shape objects stored were not compatible with Solr.", rse);
            log.error("Details in failed document batch: ");
            for (SolrInputDocument d : documents) {
                Collection<String> fieldNames = d.getFieldNames();
                for (String name : fieldNames) {
                    log.error(name + ":" + d.getFieldValue(name).toString());
                }
            }
        }
    }

    private void commitDeletes(SolrServer server, List<String> deleteIds, boolean isLastBatch) throws SolrServerException, IOException {
        int numDeletes = deleteIds.size();
        if (numDeletes == 0) {
            return;
        }

        if (numDeletes >= BATCH_SIZE || isLastBatch) {
            server.deleteById(deleteIds);
            server.commit();
            deleteIds.clear();
        }
    }


    @Override
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informaations, TransactionHandle tx) throws StorageException {
        List<String> result = new ArrayList<String>();
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
        List<IndexQuery.OrderEntry> sorts = query.getOrder();

        if (null != sorts && sorts.size() > 0) {
            for (IndexQuery.OrderEntry sortEntry : sorts) {
                String sortField = sortEntry.getKey();
                SolrQuery.ORDER sortOrder = ( sortEntry.getOrder() == Order.DESC ) ?
                        SolrQuery.ORDER.desc :
                        SolrQuery.ORDER.asc;
                solrQuery.addSort(sortField, sortOrder);
            }
        }

        try {
            QueryResponse response = null;

            response = solr.query(solrQuery);
            log.debug("Executed query [{}] in {} ms", query.getCondition(), response.getElapsedTime());
            int totalHits = response.getResults().size();
            if (false == query.hasLimit() && totalHits >= MAX_RESULT_SET_SIZE) {
                log.warn("Query result set truncated to first [{}] elements for query: {}", MAX_RESULT_SET_SIZE, query);
            }
            result = new ArrayList<String>(totalHits);
            for (SolrDocument hit : response.getResults()) {
                result.add(hit.getFieldValue(keyIdField).toString());
            }

        } catch (HttpSolrServer.RemoteSolrException e) {
            log.error("Query did not complete because parameters were not recognized : ", e);
        } catch (SolrServerException e) {
            log.error("Unable to query Solr index.", e);
        }
        return result;
    }

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, TransactionHandle tx) throws StorageException {
        List<RawQuery.Result<String>> result = new ArrayList<RawQuery.Result<String>>();
        String core = query.getStore();
        String keyIdField = keyFieldIds.get(core);
        SolrServer solr = this.solrServers.get(core);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("*:*");
        solrQuery.addFilterQuery(query.getQuery());
        solrQuery.addField(keyIdField);
        solrQuery.addField("score");

        if (query.hasLimit()) {
            solrQuery.setRows(query.getLimit());
        } else {
            solrQuery.setRows(MAX_RESULT_SET_SIZE);
        }

        try {
            QueryResponse response = null;

            response = solr.query(solrQuery);
            log.debug("Executed query [{}] in {} ms", query.getQuery(), response.getElapsedTime());
            int totalHits = response.getResults().size();
            if (false == query.hasLimit() && totalHits >= MAX_RESULT_SET_SIZE) {
                log.warn("Query result set truncated to first [{}] elements for query: {}", MAX_RESULT_SET_SIZE, query);
            }
            result = new ArrayList<RawQuery.Result<String>>(totalHits);

            for (SolrDocument hit : response.getResults()) {
                double score = Double.parseDouble(hit.getFieldValue("score").toString());
                result.add(
                        new RawQuery.Result<String>(hit.getFieldValue(keyIdField).toString(), score));
            }
        } catch (HttpSolrServer.RemoteSolrException e) {
            log.error("Query did not complete because parameters were not recognized : ", e);
        } catch (SolrServerException e) {
            log.error("Unable to query Solr index.", e);
        }
        return result;
    }


    public SolrQuery buildQuery(SolrQuery q, Condition<?> condition) {
        if (condition instanceof PredicateCondition) {
            PredicateCondition<String, ?> atom= (PredicateCondition<String, ?>) condition;
            Object value = atom.getValue();
            String key = atom.getKey();
            TitanPredicate titanPredicate = atom.getPredicate();

            if (value instanceof Number
                //|| value instanceof Interval
                ) {

                Preconditions.checkArgument(titanPredicate instanceof Cmp, "Relation not supported on numeric types: " + titanPredicate);
                Cmp numRel = (Cmp) titanPredicate;
                //if (numRel == Cmp.INTERVAL) {
                //    Interval i = (Interval)value;
                //    q.addFilterQuery(key + ":[" + i.getStart() + " TO " + i.getEnd() + "]");
                //    return q;
                //} else {
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
                //}
            } else if (value instanceof String) {
                if (titanPredicate == Text.CONTAINS) {
                    //e.g. - if terms tomorrow and world were supplied, and fq=text:(tomorrow  world)
                    //sample data set would return 2 documents: one where text = Tomorrow is the World,
                    //and the second where text = Hello World
                    q.addFilterQuery(key + ":("+((String) value).toLowerCase()+")");
                    return q;
                } else if (titanPredicate == Text.PREFIX) {
                    String prefixConventionName = "String";
                    q.addFilterQuery(key + prefixConventionName + ":"+((String) value)+"*");
                    return q;
                } else if (titanPredicate == Text.REGEX) {
                    String prefixConventionName = "String";
                    q.addFilterQuery(key + prefixConventionName + ":/"+((String) value)+"/");
                    return q;
                } else if (titanPredicate == Text.CONTAINS_PREFIX) {
                    q.addFilterQuery(key + ":"+((String) value)+"*");
                    return q;
                } else if (titanPredicate == Cmp.EQUAL) {
                    q.addFilterQuery(key + ":\"" +((String)value)+"\"");
                    return q;
                } else if (titanPredicate == Cmp.NOT_EQUAL) {
                    q.addFilterQuery("-" + key + ":\"" +((String)value)+"\"");
                    return q;
                } else if (titanPredicate == Text.CONTAINS_REGEX) {
                    q.addFilterQuery(key + ":/"+((String) value)+"/");
                    return q;
                } else {
                    throw new IllegalArgumentException("Relation is not supported for string value: " + titanPredicate);
                }
            } else if (value instanceof Geoshape) {
                Geoshape geo = (Geoshape)value;
                if (geo.getType() == Geoshape.Type.CIRCLE) {
                    Geoshape.Point center = geo.getPoint();
                    q.addFilterQuery("{!geofilt sfield=" + key +
                            " pt=" + center.getLatitude() + "," + center.getLongitude() +
                            " d=" + geo.getRadius() + "} distErrPct=0"); //distance in kilometers
                    return q;
                } else if (geo.getType() == Geoshape.Type.BOX) {
                    Geoshape.Point southwest = geo.getPoint(0);
                    Geoshape.Point northeast = geo.getPoint(1);
                    q.addFilterQuery(key + ":[" + southwest.getLatitude() + "," + southwest.getLongitude() +
                                    " TO " + northeast.getLatitude() + "," + northeast.getLongitude() + "]");
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
        } else if (condition instanceof Not) {
            String[] filterConditions = q.getFilterQueries();
           for (String filterCondition : filterConditions) {
                //if (filterCondition.contains(key)) {
                    q.removeFilterQuery(filterCondition);
                    q.addFilterQuery("-" + filterCondition);
                //}
            }
            return q;
        } else if (condition instanceof And) {

            for (Condition c : condition.getChildren()) {
                SolrQuery andCondition = new SolrQuery();
                andCondition.setQuery("*:*");
                andCondition =  buildQuery(andCondition, c);
                String[] andFilterConditions = andCondition.getFilterQueries();
                for (String filter : andFilterConditions) {
                    //+ in solr makes the condition required
                    q.addFilterQuery("+" + filter);
                }
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
                server.commit();
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
    public boolean supports(KeyInformation information, TitanPredicate titanPredicate) {
        Class<?> dataType = information.getDataType();

        if (Number.class.isAssignableFrom(dataType)) {
            if (titanPredicate instanceof Cmp) {
                return true;
            } else {
                return false;
            }
        } else if (dataType == Geoshape.class) {
            return titanPredicate == Geo.WITHIN;
        } else if (dataType == String.class
                && (titanPredicate == Text.CONTAINS ||
                titanPredicate == Text.PREFIX ||
                titanPredicate == Text.REGEX ||
                titanPredicate == Text.CONTAINS_PREFIX ||
                titanPredicate == Text.CONTAINS_REGEX ||
                titanPredicate == Cmp.EQUAL ||
                titanPredicate == Cmp.NOT_EQUAL)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean supports(KeyInformation information) {
        Class<?> dataType = information.getDataType();
        if (Number.class.isAssignableFrom(dataType) || dataType == Geoshape.class) {
            return true;
        } else if (AttributeUtil.isString(dataType)) {
            return true;
        }
        return false;
    }

    private StorageException storageException(Exception solrException) {
        return new TemporaryStorageException("Unable to complete query on Solr.", solrException);
    }

}
