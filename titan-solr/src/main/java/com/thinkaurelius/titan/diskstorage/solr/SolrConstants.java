package com.thinkaurelius.titan.diskstorage.solr;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class SolrConstants {

    //Global settings
    public static final String SOLR_MODE = "solr.mode";
    public static final String SOLR_MODE_EMBEDDED = "embedded";
    public static final String SOLR_MODE_HTTP = "http";
    public static final String SOLR_MODE_CLOUD = "cloud";
    public static final String SOLR_COMMIT_BATCH_SIZE = "solr.commit.batch.size";

    //Field name that uniquely identifies each document in Solr
    public static final String SOLR_KEY_FIELD_NAMES = "solr.key.field.names";
    public static final String SOLR_DEFAULT_KEY_FIELD = "docid";

    //Embedded config settings
    public static final String SOLR_CORE_NAMES = "solr.core.names";
    public static final String SOLR_EMBEDDED_DEFAULT_CORE_NAME = "titan";
    public static final String SOLR_HOME = "solr.solr.home";

    //Http Solr Server config settings
    public static final String SOLR_HTTP_URL = "solr.http.url";
    public static final String SOLR_HTTP_DEFAULT_URL = "http://localhost:8983/solr";
    public static final String SOLR_HTTP_CONNECTION_TIMEOUT = "solr.http.connection.timeout";
    public static final int SOLR_HTTP_CONNECTION_DEFAULT_TIMEOUT = 5000;
    public static final String SOLR_HTTP_CONNECTION_ALLOW_COMPRESSION = "solr.http.connection.allowCompression";
    public static final boolean SOLR_HTTP_CONNECTION_DEFAULT_ALLLOW_COMPRESSION = false;
    public static final String SOLR_HTTP_CONNECTION_MAX_CONNECTIONS = "solr.http.connection.maxConnections";
    public static final int SOLR_HTTP_CONNECTION_DEFAULT_MAX_CONNECTIONS = 100;
    public static final String SOLR_HTTP_CONNECTION_MAX_CONNECTIONS_PER_HOST = "solr.http.connection.maxConnectionsPerHost";
    public static final int SOLR_HTTP_CONNECTION_DEFAULT_MAX_CONNECTIONS_PER_HOST = 100;

    //Solr Cloud config settings
    public static final String SOLR_CLOUD_ZOOKEEPER_URL = "solr.cloud.zookeeper.url";
    public static final String SOLR_CLOUD_DEFAULT_ZOOKEEPER_URL = "http://localhost:9983";
    public static final String SOLR_CLOUD_COLLECTION = "solr.cloud.defaultCollection";
    public static final String SOLR_CLOUD_DEFAULT_COLLECTION = "collection1";


}
