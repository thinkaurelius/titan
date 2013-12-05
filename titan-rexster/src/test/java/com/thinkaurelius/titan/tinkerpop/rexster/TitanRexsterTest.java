package com.thinkaurelius.titan.tinkerpop.rexster;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.rexster.client.HintedRexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.protocol.EngineConfiguration;
import com.tinkerpop.rexster.protocol.EngineController;
import com.tinkerpop.rexster.server.RexProRexsterServer;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterCluster;
import com.tinkerpop.rexster.server.RexsterCommandLine;
import com.tinkerpop.rexster.server.RexsterServer;
import com.tinkerpop.rexster.server.RexsterSettings;
import com.tinkerpop.rexster.server.XmlRexsterApplication;

public class TitanRexsterTest {

    public static final String GRAPH_NAME = "graph";
    public static final List<String> ALLOWABLE_NAMESPACES = ImmutableList.of("*:*");
    public static final List<HierarchicalConfiguration> EXTENSION_CONFIGURATIONS =
            ImmutableList.of();
    
    private static RexsterApplication app;
    private static RexsterServer rexproServer;
    private static RexsterServer clusterServer;
    
    @BeforeClass
    public static void setUpRexsterServer() throws Exception {
        
        String args[] = new String[] { "-s", "-wr", "public", "-c", "target/test-classes/rexster-bdb.xml"};
        
        final RexsterSettings settings = new RexsterSettings(args);
        final RexsterCommandLine line = settings.getCommand();
        
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
        
        // Start JGroups broadcaster/listener
        clusterServer = new RexsterCluster(settings.getProperties());
        
        System.out.println("<<<XZY>>>");
    }
    
    private static Configuration getGraphConfig() {
        Configuration c = new BaseConfiguration();
        c.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "berkeleyje");
        c.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, "target" + File.separator + "db");
        return c;
    }
    
    @AfterClass
    public static void tearDownRexsterServer() throws Exception {
        
        clusterServer.stop();
        rexproServer.stop();
    }
    
    @Test
    public void what() throws Exception {
        HintedRexsterClient client = RexsterClientFactory.openHinted(null);
        
        long vid = 4;
        
        final HintedRexsterClient.Hint<Long> hint = new HintedRexsterClient.Hint<Long>(Vertex.class, null, GRAPH_NAME);
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("x", vid);
        client.execute("g=rexster.getGraph('" + GRAPH_NAME + "');g.addVertex(null);", bindings, hint);
//        
//        List l = client.execute("g=rexster.getGraph('" + GRAPH_NAME + "');g.v(x)", bindings, hint);
//        for (Object i : l) {
//            System.out.println(i);
//        }
//        System.out.println("worked..........next");
    }
}
