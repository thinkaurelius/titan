package com.thinkaurelius.titan.diskstorage.solr;

import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

import static com.thinkaurelius.titan.diskstorage.solr.SolrConstants.SOLR_CORE_NAMES;
import static com.thinkaurelius.titan.diskstorage.solr.SolrConstants.SOLR_EMBEDDED_DEFAULT_CORE_NAME;

public class SolrUtils {

    public static List<String> parseConfigForCoreNames(Configuration config) {
        List<String> defaultCoreNames = new ArrayList<String>();
        defaultCoreNames.add(SOLR_EMBEDDED_DEFAULT_CORE_NAME);
        return config.getList(SOLR_CORE_NAMES, defaultCoreNames);
    }

}
