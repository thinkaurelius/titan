package com.thinkaurelius.titan.example.accumulo;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import java.io.File;
import java.io.IOException;

/**
 * Example Graph factory that creates a {@link TitanGraph} based on roman
 * mythology. Used in the documentation examples and tutorials.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphOfTheGodsFactory {

    public static final String INDEX_NAME = "search";

    public static void main(String[] args) throws IOException {
        TitanGraph graph;

        System.out.println("Creating graph ... ");
        graph = open(args[0]);
        System.out.println("Loading graph ... ");
        load(graph);
        System.out.println("Done!");
    }
    
    public static TitanGraph create(final String directory) {
        TitanGraph graph;
        
        graph = open(directory);
        load(graph);
        return graph;
    }

    public static TitanGraph open(final String directory) {
        BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        // configuring local backend
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,
                "com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager");
        storage.setProperty(GraphDatabaseConfiguration.HOSTNAME_KEY, "localhost");

        Configuration accumulo = storage.subset(AccumuloStoreManager.ACCUMULO_CONFIGURATION_NAMESPACE);

        accumulo.addProperty(AccumuloStoreManager.ACCUMULO_INTSANCE_KEY, "EtCloud");
        accumulo.addProperty(GraphDatabaseConfiguration.HOSTNAME_KEY, "localhost");

        accumulo.addProperty(AccumuloStoreManager.ACCUMULO_USER_KEY, "root");
        accumulo.addProperty(AccumuloStoreManager.ACCUMULO_PASSWORD_KEY, "bobross");
        // configuring elastic search index
        Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
        index.setProperty(GraphDatabaseConfiguration.INDEX_BACKEND_KEY, "lucene");
        /*
        index.setProperty("local-mode", true);
        index.setProperty("client-only", false);
        */
        index.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, directory + File.separator + "lucene");

        TitanGraph graph = TitanFactory.open(config);

        return graph;
    }

    public static void load(final TitanGraph graph) {

        graph.makeType().name("name").dataType(String.class).indexed(Vertex.class).unique(Direction.BOTH).makePropertyKey();
        graph.makeType().name("age").dataType(Integer.class).indexed(INDEX_NAME, Vertex.class).unique(Direction.OUT).makePropertyKey();
        graph.makeType().name("type").dataType(String.class).unique(Direction.OUT).makePropertyKey();

        final TitanKey time = graph.makeType().name("time").dataType(Integer.class).unique(Direction.OUT).makePropertyKey();
        final TitanKey reason = graph.makeType().name("reason").dataType(String.class).indexed(INDEX_NAME, Edge.class).unique(Direction.OUT).makePropertyKey();
        graph.makeType().name("place").dataType(Geoshape.class).indexed(INDEX_NAME, Edge.class).unique(Direction.OUT).makePropertyKey();

        graph.makeType().name("father").unique(Direction.OUT).makeEdgeLabel();
        graph.makeType().name("mother").unique(Direction.OUT).makeEdgeLabel();
        graph.makeType().name("battled").primaryKey(time).makeEdgeLabel();
        graph.makeType().name("lives").signature(reason).makeEdgeLabel();
        graph.makeType().name("pet").makeEdgeLabel();
        graph.makeType().name("brother").makeEdgeLabel();

        graph.commit();

        // vertices

        Vertex saturn = graph.addVertex(null);
        saturn.setProperty("name", "saturn");
        saturn.setProperty("age", 10000);
        saturn.setProperty("type", "titan");

        Vertex sky = graph.addVertex(null);
        ElementHelper.setProperties(sky, "name", "sky", "type", "location");

        Vertex sea = graph.addVertex(null);
        ElementHelper.setProperties(sea, "name", "sea", "type", "location");

        Vertex jupiter = graph.addVertex(null);
        ElementHelper.setProperties(jupiter, "name", "jupiter", "age", 5000, "type", "god");

        Vertex neptune = graph.addVertex(null);
        ElementHelper.setProperties(neptune, "name", "neptune", "age", 4500, "type", "god");

        Vertex hercules = graph.addVertex(null);
        ElementHelper.setProperties(hercules, "name", "hercules", "age", 30, "type", "demigod");

        Vertex alcmene = graph.addVertex(null);
        ElementHelper.setProperties(alcmene, "name", "alcmene", "age", 45, "type", "human");

        Vertex pluto = graph.addVertex(null);
        ElementHelper.setProperties(pluto, "name", "pluto", "age", 4000, "type", "god");

        Vertex nemean = graph.addVertex(null);
        ElementHelper.setProperties(nemean, "name", "nemean", "type", "monster");

        Vertex hydra = graph.addVertex(null);
        ElementHelper.setProperties(hydra, "name", "hydra", "type", "monster");

        Vertex cerberus = graph.addVertex(null);
        ElementHelper.setProperties(cerberus, "name", "cerberus", "type", "monster");

        Vertex tartarus = graph.addVertex(null);
        ElementHelper.setProperties(tartarus, "name", "tartarus", "type", "location");

        // edges

        jupiter.addEdge("father", saturn);
        jupiter.addEdge("lives", sky).setProperty("reason", "loves fresh breezes");
        jupiter.addEdge("brother", neptune);
        jupiter.addEdge("brother", pluto);

        neptune.addEdge("lives", sea).setProperty("reason", "loves waves");
        neptune.addEdge("brother", jupiter);
        neptune.addEdge("brother", pluto);

        hercules.addEdge("father", jupiter);
        hercules.addEdge("mother", alcmene);
        ElementHelper.setProperties(hercules.addEdge("battled", nemean), "time", 1, "place", Geoshape.point(38.1f, 23.7f));
        ElementHelper.setProperties(hercules.addEdge("battled", hydra), "time", 2, "place", Geoshape.point(37.7f, 23.9f));
        ElementHelper.setProperties(hercules.addEdge("battled", cerberus), "time", 12, "place", Geoshape.point(39f, 22f));

        pluto.addEdge("brother", jupiter);
        pluto.addEdge("brother", neptune);
        pluto.addEdge("lives", tartarus).setProperty("reason", "no fear of death");
        pluto.addEdge("pet", cerberus);

        cerberus.addEdge("lives", tartarus);

        // commit the transaction to disk
        graph.commit();
    }
}
