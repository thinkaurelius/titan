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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.LoggerFactory;

/**
 * Example Graph factory that creates a {@link TitanGraph} based on roman
 * mythology. Used in the documentation examples and tutorials.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphOfTheGodsFactory {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(GraphOfTheGodsFactory.class);
    private static final String ZOOKEEPERS_DEFAULT = "localhost";
    private static final String USERNAME_DEFAULT = "root";
    private String instance;
    private String zooKeepers;
    private String username;
    private String password;

    public TitanGraph create() {
        return create(instance, zooKeepers, username, password);
    }

    public static TitanGraph create(final String instance, final String zooKeepers,
            final String username, final String password) {

        return load(define(open(instance, zooKeepers, username, password)));
    }

    public TitanGraph open() {
        return open(instance, zooKeepers, username, password);
    }

    public static TitanGraph open(final String instance, final String zooKeepers,
            final String username, final String password) {
        BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        // configuring local backend
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,
                "com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager");
        storage.setProperty(GraphDatabaseConfiguration.HOSTNAME_KEY, "localhost");

        Configuration accumulo = storage.subset(AccumuloStoreManager.ACCUMULO_CONFIGURATION_NAMESPACE);

        accumulo.addProperty(AccumuloStoreManager.ACCUMULO_INTSANCE_KEY, instance);

        accumulo.addProperty(AccumuloStoreManager.ACCUMULO_USER_KEY, username);
        accumulo.addProperty(AccumuloStoreManager.ACCUMULO_PASSWORD_KEY, password);

        TitanGraph graph = TitanFactory.open(config);

        return graph;
    }

    public static TitanGraph define(final TitanGraph graph) {
        // Define types
        graph.makeType().name("name").dataType(String.class).indexed(Vertex.class).unique(Direction.BOTH).makePropertyKey();
        graph.makeType().name("age").dataType(Integer.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
        graph.makeType().name("type").dataType(String.class).unique(Direction.OUT).makePropertyKey();

        final TitanKey time = graph.makeType().name("time").dataType(Integer.class).unique(Direction.OUT).makePropertyKey();
        final TitanKey reason = graph.makeType().name("reason").dataType(String.class).indexed(Edge.class).unique(Direction.OUT).makePropertyKey();
        graph.makeType().name("place").dataType(Geoshape.class).indexed(Edge.class).unique(Direction.OUT).makePropertyKey();

        graph.makeType().name("father").unique(Direction.OUT).makeEdgeLabel();
        graph.makeType().name("mother").unique(Direction.OUT).makeEdgeLabel();
        graph.makeType().name("battled").primaryKey(time).makeEdgeLabel();
        graph.makeType().name("lives").signature(reason).makeEdgeLabel();
        graph.makeType().name("pet").makeEdgeLabel();
        graph.makeType().name("brother").makeEdgeLabel();

        graph.commit();

        return graph;
    }

    public static TitanGraph load(final TitanGraph graph) {

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

        graph.commit();

        return graph;
    }

    private static Collection<Option> getOptions() {
        Option instanceOpt = OptionBuilder
                .isRequired()
                .hasArg()
                .withArgName("name")
                .withDescription("Accumulo instance")
                .create("instance");

        Option zooKeepersOpt = OptionBuilder
                .hasArg()
                .withArgName("quorum")
                .withDescription("Zookeeper quorum")
                .create("zooKeepers");

        Option userOpt = OptionBuilder
                .hasArg()
                .withArgName("name")
                .withDescription("Accumulo user")
                .create("user");

        Option passwordOpt = OptionBuilder
                .isRequired()
                .hasArg()
                .withArgName("password")
                .withDescription("Accumulo password")
                .create("password");

        List<Option> options = new ArrayList<Option>();
        
        options.add(instanceOpt);
        options.add(zooKeepersOpt);
        options.add(userOpt);
        options.add(passwordOpt);

        return options;
    }
    
    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("GraphOfTheGodsFactory", options);
    }

    private void parseArgs(String[] args) {
        Options options = null;
        try {        
            CommandLineParser parser = new PosixParser();
            options = new Options();
            options.addOption("help", false, "display this help");
            
            CommandLine line = parser.parse(options, args, true);
            for (Option option : getOptions()) {
                options.addOption(option);
            }
            
            if (line.hasOption("help")) {
                printHelp(options);
                System.exit(0);
            }
            
            line = parser.parse(options, args);
      
            instance = line.getOptionValue("instance");
            zooKeepers = line.getOptionValue("zooKeepers", ZOOKEEPERS_DEFAULT);
            username = line.getOptionValue("user", USERNAME_DEFAULT);
            password = line.getOptionValue("password");
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            printHelp(options);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws IOException {
        GraphOfTheGodsFactory gotgf = new GraphOfTheGodsFactory();

        gotgf.parseArgs(args);

        TitanGraph graph;
        System.out.println("Creating graph ... ");
        graph = define(gotgf.open());
        System.out.println("Loading graph ... ");
        load(graph);
        System.out.println("Done!");
    }
}
