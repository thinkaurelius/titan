package com.thinkaurelius.titan.diskstorage.solr;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

import static com.thinkaurelius.titan.diskstorage.solr.SolrSearchConstants.*;

/**
 * Generates an EmbeddedSolrServer, HttpSolrServer, or CloudSolrServer based on
 * configuration settings provided.
 */
public class SolrServerFactory {

    private Logger log = LoggerFactory.getLogger(SolrServerFactory.class);




    public Map<String, SolrServer> buildSolrServers(Configuration config)
            throws IllegalArgumentException, SolrException {
        Map<String, SolrServer> solrServers;

        String mode = config.getString(SOLR_MODE, SOLR_MODE_EMBEDDED);

        if (mode.equalsIgnoreCase(SOLR_MODE_EMBEDDED)) {
            solrServers = buildEmbeddedSolrServers(config);
        } else if (mode.equalsIgnoreCase(SOLR_MODE_HTTP)) {
            solrServers = buildHttpSolrServer(config);
        } else if (mode.equalsIgnoreCase(SOLR_MODE_CLOUD)) {
            solrServers = buildCloudSolrServer(config);
        } else {
            throw new IllegalArgumentException(
                    "Unable to determine the type of Solr connection needed. " +
                            " The solr-mode configuration key must be set to one of : " +
                            "embedded, http, or cloud to generate the appropriate connection to Solr.");
        }

        return solrServers;
    }

    /**
     * Generates an EmbeddedSolrServer connection. Note that mult-core embedded is not supported in this method.
     * @param config
     * @return
     * @throws SolrException
     */
    private Map<String, SolrServer> buildEmbeddedSolrServers(Configuration config)
        throws SolrException {
        Map<String, SolrServer> servers = new HashMap<String, SolrServer>();

        String solrHome = config.getString(SOLR_HOME, "");
        File home = new File(solrHome);
        CoreContainer coreContainer = new CoreContainer(home.getAbsolutePath());

        List<String> coreNames = SolrSearchUtils.parseConfigForCoreNames(config);

        for (String coreName : coreNames) {
            Properties coreProperties = new Properties();
            coreProperties.put(CoreDescriptor.CORE_NAME, coreName);
            coreProperties.put(CoreDescriptor.CORE_INSTDIR, home.getAbsolutePath() + "/" + coreName);
            coreProperties.put(CoreDescriptor.CORE_PROPERTIES, "core.properties");

            CoreDescriptor cd = new CoreDescriptor(coreContainer, coreProperties);
            SolrCore newCore = coreContainer.create(cd);
            coreContainer.register(newCore, true);
            coreContainer.load();
            SolrServer server = new EmbeddedSolrServer(coreContainer, coreName);
            servers.put(coreName, server);

            log.debug("Using core name of: " + coreName + " for EmbeddedSolrServer. This value can be changed by setting the solr.embedded.coreName value");
        }

        return servers;
    }


    private Map<String, SolrServer> buildHttpSolrServer(Configuration config)
            throws SolrException {
        HttpSolrServer server = null;



        String url = config.getString(SOLR_HTTP_URL, SOLR_HTTP_DEFAULT_URL);
        server = new HttpSolrServer(url);
        server.setConnectionTimeout(config.getInt(SOLR_HTTP_CONNECTION_TIMEOUT,SOLR_HTTP_CONNECTION_DEFAULT_TIMEOUT));
        server.setAllowCompression(config.getBoolean(SOLR_HTTP_CONNECTION_ALLOW_COMPRESSION, SOLR_HTTP_CONNECTION_DEFAULT_ALLLOW_COMPRESSION));
        server.setDefaultMaxConnectionsPerHost(config.getInt(SOLR_HTTP_CONNECTION_MAX_CONNECTIONS, SOLR_HTTP_CONNECTION_DEFAULT_MAX_CONNECTIONS_PER_HOST));
        server.setMaxTotalConnections(config.getInt(SOLR_HTTP_CONNECTION_MAX_CONNECTIONS, SOLR_HTTP_CONNECTION_DEFAULT_MAX_CONNECTIONS));

        List<String> coreNames = SolrSearchUtils.parseConfigForCoreNames(config);

        createCoreIfNotExists(coreNames, server);
        Map<String, SolrServer> servers = new HashMap<String, SolrServer>();
        for (String coreName : coreNames) {
            servers.put(coreName, server);
        }
        return servers;
    }



    private void createCoreIfNotExists(List<String> coreNames, SolrServer server) {
        if (coreNames == null || coreNames.size() == 0) {
            return;
        }

        for (String coreName : coreNames) {
            CoreAdminRequest coreRequest = new CoreAdminRequest();
            coreRequest.setAction(CoreAdminParams.CoreAdminAction.STATUS);
            try {
                NamedList<Object> coreStatus = null;
                try {
                    CoreAdminResponse statusResponse = coreRequest.process(server);
                    coreStatus = statusResponse.getCoreStatus(coreName);
                } catch (SolrException e) {
                    log.error("Unable to successfully determine status of cores on solr server provided.", e);
                }

                if (coreStatus != null) {
                    log.trace("Core {} already created.", coreName);
                } else {

                    coreRequest = new CoreAdminRequest();
                    coreRequest.setCoreName(coreName);
                    coreRequest.setAction(CoreAdminParams.CoreAdminAction.CREATE);
                    try {
                        CoreAdminResponse createResponse = coreRequest.process(server);
                        if (createResponse.getUptime(coreName) > 0L) {
                            log.trace("Core {} successfully created.", coreName);
                        }

                    } catch (Exception e) {
                        log.error("Unable to create requested core " +  coreName, e);
                    }
                }
            } catch (SolrServerException e) {
                log.error("Unable to successfully determine status of cores on solr server provided.", e);
            } catch (IOException e) {
                log.error("Unable to establish connection with solr server.", e);
            }
        }

    }

    private Map<String, SolrServer> buildCloudSolrServer(Configuration config)
            throws SolrException {
        CloudSolrServer server = null;
        Map<String, SolrServer> servers = new HashMap<String, SolrServer>();

        String zookeeperUrl = config.getString(SOLR_CLOUD_ZOOKEEPER_URL, SOLR_CLOUD_DEFAULT_ZOOKEEPER_URL);
        try {
            server = new CloudSolrServer(zookeeperUrl);
            server.setDefaultCollection(config.getString(SOLR_CLOUD_COLLECTION, SOLR_CLOUD_DEFAULT_COLLECTION));

            List<String> coreNames = SolrSearchUtils.parseConfigForCoreNames(config);

            createCoreIfNotExists(coreNames, server);

            for (String coreName : coreNames) {
                servers.put(coreName, server);
            }
        } catch (MalformedURLException e) {
            String message = "Could not create Cloud Solr Server connection because of a bad zookeepr url provided: " + zookeeperUrl;
            log.error(message, e);
            throw new SolrException(SolrException.ErrorCode.UNKNOWN, message, e);
        }
        return servers;
    }
}
