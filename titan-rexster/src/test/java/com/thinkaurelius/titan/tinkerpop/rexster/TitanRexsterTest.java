package com.thinkaurelius.titan.tinkerpop.rexster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.rexster.client.HintedRexsterClient;
import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.protocol.EngineConfiguration;
import com.tinkerpop.rexster.protocol.EngineController;
import com.tinkerpop.rexster.server.RexProRexsterServer;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterCluster;
import com.tinkerpop.rexster.server.RexsterServer;
import com.tinkerpop.rexster.server.RexsterSettings;
import com.tinkerpop.rexster.server.XmlRexsterApplication;

public class TitanRexsterTest {

    public static final String GRAPH_NAME = "graph";
    public static final List<String> ALLOWABLE_NAMESPACES = ImmutableList.of("*:*");
    public static final List<HierarchicalConfiguration> EXTENSION_CONFIGURATIONS =
            ImmutableList.of();
    
    private static final Logger log = LoggerFactory.getLogger(TitanRexsterTest.class);
    
    private RexsterApplication app;
    private RexsterServer rexproServer;
    private RexsterServer clusterServer;
    private HintedRexsterClient client;
    
    @Before
    public void setUpRexsterServer() throws Exception {
        
        final File dbDir = new File("target" + File.separator + "db");
        final boolean dbDirExisted = dbDir.exists();

        FileUtils.deleteQuietly(dbDir);

        if (dbDirExisted && dbDir.exists()) {
            log.warn("Unable to delete directory {}" , dbDir);
        }

        String args[] = new String[] { "-s", "-wr", "public", "-c",
                Joiner.on(File.separator).join("target", "test-classes", "rexster-bdb.xml") };
        
        final RexsterSettings settings = new RexsterSettings(args);
        
        app = new XmlRexsterApplication(settings.getProperties());
        
        // Configure script engine(s)
        final List<EngineConfiguration> configuredScriptEngines = new ArrayList<EngineConfiguration>();
        final List<HierarchicalConfiguration> configs = settings.getProperties().getScriptEngines();
        for(HierarchicalConfiguration config : configs) {
            configuredScriptEngines.add(new EngineConfiguration(config));
        }
        EngineController.configure(configuredScriptEngines);
        
        // Start rexpro listener
        rexproServer = new RexProRexsterServer(settings.getProperties(), true);
        rexproServer.start(app);
        
        // Start JGroups broadcaster/listener
        clusterServer = new RexsterCluster(settings.getProperties());
        clusterServer.start(app);
        
        log.info("RexPro and RexCluster started");

        client = RexsterClientFactory.openHinted(null);
        
        log.info("RexsterClient started");
    }
    
    @After
    public void tearDownRexsterServer() throws Exception {
        
        log.info("Stopping RexPro and RexCluster...");
        
        clusterServer.stop();
        rexproServer.stop();
        
        log.info("RexPro and RexCluster stopped");
        
        client.close();
        
        log.info("RexsterClient started");
    }
    
    @Test
    public void testSimpleUnhinted() throws Exception {
        Map<String, Object> bindings = new HashMap<String, Object>();
        long count = 20;
        bindings.put("count", count);
        client.execute("g=rexster.getGraph('" + GRAPH_NAME + "'); for (i in 1..count) { g.addVertex(null); }; g.commit();", bindings, null);
        List<?> l = client.execute("g=rexster.getGraph('" + GRAPH_NAME + "');g.V.count()", bindings, null);
        assertNotNull(l);
        assertEquals(1, l.size());
        assertEquals(count, l.get(0));
    }
    
    @Test
    public void testSimpleHinted() throws RexProException, IOException {
        final HintedRexsterClient.Hint<StaticBuffer> hint =
              new HintedRexsterClient.Hint<StaticBuffer>(Vertex.class, ByteBufferUtil.getLongBuffer(4), GRAPH_NAME);
        Map<String, Object> bindings = ImmutableMap.of();
        List<?> l = client.execute("g=rexster.getGraph('" + GRAPH_NAME + "'); g.V.count()", bindings, hint);
        assertNotNull(l);
        assertEquals(1, l.size());
        assertEquals(0L, l.get(0));
    }
}
