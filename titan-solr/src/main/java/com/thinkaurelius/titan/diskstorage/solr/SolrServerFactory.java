package com.thinkaurelius.titan.diskstorage.solr;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.MalformedURLException;

/**
 * Generates an EmbeddedSolrServer, HttpSolrServer, or CloudSolrServer based on
 * configuration settings provided.
 */
public class SolrServerFactory {

    private Logger log = LoggerFactory.getLogger(SolrServerFactory.class);

    public static final String SOLR_MODE = "solr-mode";
    public static final String SOLR_MODE_EMBEDDED = "embedded";
    public static final String SOLR_MODE_HTTP = "http";
    public static final String SOLR_MODE_CLOUD = "cloud";

    //Embedded config settings
    public static final String SOLR_EMBEDDED_CORE_NAME = "solr.embedded.coreName";
    public static final String SOLR_EMBEDDED_DEFAULT_CORE_NAME = "titan";

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


    public SolrServer buildSolrServer(Configuration config)
            throws IllegalArgumentException, SolrException {
        SolrServer solrServer = null;
        String mode = config.getString(SOLR_MODE);
        if (StringUtils.isBlank(mode)) {
            mode = SOLR_MODE_EMBEDDED;
        }

        if (mode.equalsIgnoreCase(SOLR_MODE_EMBEDDED)) {
            solrServer = buildEmbeddedSolrServer(config);
        } else if (mode.equalsIgnoreCase(SOLR_MODE_HTTP)) {
            solrServer = buildHttpSolrServer(config);
        } else if (mode.equalsIgnoreCase(SOLR_MODE_CLOUD)) {
            solrServer = buildCloudSolrServer(config);
        } else {
            throw new IllegalArgumentException(
                    "Unable to determine the type of Solr connection needed. " +
                            " The solr-mode configuration key must be set to one of : " +
                            "embedded, http, or cloud to generate the appropriate connection to Solr.");
        }

        return solrServer;
    }

    /**
     * Generates an EmbeddedSolrServer connection. Note that mult-core embedded is not supported in this method.
     * @param config
     * @return
     * @throws SolrException
     */
    private SolrServer buildEmbeddedSolrServer(Configuration config)
        throws SolrException {
        SolrServer server = null;
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        try {
            CoreContainer coreContainer = initializer.initialize();
            String coreName = config.getString(SOLR_EMBEDDED_CORE_NAME, SOLR_EMBEDDED_DEFAULT_CORE_NAME);
            log.debug("Using core name of: " + coreName + " for EmbeddedSolrServer. This value can be changed by setting the solr.embedded.coreName value");
            server = new EmbeddedSolrServer(coreContainer, coreName);
        } catch (FileNotFoundException e) {                                            e.printStackTrace();
            log.error("Unable to locate home directory for Embedded Solr Instance. Make sure to set the system property solr.solr.home or provide embedded instance of solr in the solr/ directory under the working directory.");
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Unable to generate embedded solr server connection.", e);
        }

        return server;
    }

    private SolrServer buildHttpSolrServer(Configuration config)
            throws SolrException {
        HttpSolrServer server = null;
        String url = config.getString(SOLR_HTTP_URL, SOLR_HTTP_DEFAULT_URL);
        server = new HttpSolrServer(url);
        server.setConnectionTimeout(config.getInt(SOLR_HTTP_CONNECTION_TIMEOUT,SOLR_HTTP_CONNECTION_DEFAULT_TIMEOUT));
        server.setAllowCompression(config.getBoolean(SOLR_HTTP_CONNECTION_ALLOW_COMPRESSION, SOLR_HTTP_CONNECTION_DEFAULT_ALLLOW_COMPRESSION));
        server.setDefaultMaxConnectionsPerHost(config.getInt(SOLR_HTTP_CONNECTION_MAX_CONNECTIONS, SOLR_HTTP_CONNECTION_DEFAULT_MAX_CONNECTIONS_PER_HOST));
        server.setMaxTotalConnections(config.getInt(SOLR_HTTP_CONNECTION_MAX_CONNECTIONS, SOLR_HTTP_CONNECTION_DEFAULT_MAX_CONNECTIONS));

        return server;
    }

    private SolrServer buildCloudSolrServer(Configuration config)
            throws SolrException {
        CloudSolrServer server = null;
        String zookeeperUrl = config.getString(SOLR_CLOUD_ZOOKEEPER_URL, SOLR_CLOUD_DEFAULT_ZOOKEEPER_URL);
        try {
            server = new CloudSolrServer(zookeeperUrl);
            server.setDefaultCollection(config.getString(SOLR_CLOUD_COLLECTION, SOLR_CLOUD_DEFAULT_COLLECTION));
        } catch (MalformedURLException e) {
            String message = "Could not create Cloud Solr Server connection because of a bad zookeepr url provided: " + zookeeperUrl;
            log.error(message, e);
            throw new SolrException(SolrException.ErrorCode.UNKNOWN, message, e);
        }
        return server;
    }
}
