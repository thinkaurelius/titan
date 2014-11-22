package com.thinkaurelius.titan.hadoop.formats.cassandra;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.HadoopPipeline;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraInputFormatTest {

    private static final String KEYSPACE_NAME = TitanCassandraOutputFormatTest.class.getSimpleName();

    @BeforeClass
    public static void startUpCassandra() throws Exception {
        CassandraStorageSetup.startCleanEmbedded();
    }

    private ModifiableConfiguration getTitanConfiguration() {
        ModifiableConfiguration mc = CassandraStorageSetup.getCassandraThriftConfiguration(KEYSPACE_NAME);
        mc.set(STORAGE_HOSTS, new String[]{"localhost"});
        mc.set(DB_CACHE, false);
        return mc;
    }

    @Before
    public void clear() throws BackendException {
        ModifiableConfiguration mc = getTitanConfiguration();
        mc.set(UNIQUE_INSTANCE_ID, "deleter");
        mc.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, "tmp");
        Backend backend = new Backend(mc);
        backend.initialize(mc);
        backend.clearStorage();
        backend.close();
    }

    @Test
    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(TitanCassandraInputFormat.class.getPackage().getName() + ".*"));
    }

    @Test
    public void testCanReadListValuedProperty() throws Exception {
        TitanGraph tg = TitanFactory.open(getTitanConfiguration());

        TitanManagement mgmt = tg.getManagementSystem();
        mgmt.makePropertyKey("email").dataType(String.class).cardinality(com.thinkaurelius.titan.core.Cardinality.LIST).make();
        mgmt.commit();
        tg.commit();

        TitanVertex v = tg.addVertex();
        v.addProperty("email", "one");
        v.addProperty("email", "two");
        tg.commit();

        Configuration c = new Configuration();
        c.set("titan.hadoop.input.format", "com.thinkaurelius.titan.hadoop.formats.cassandra.TitanCassandraInputFormat");
        c.set("titan.hadoop.input.conf.storage.backend", "cassandrathrift");
        c.set("titan.hadoop.input.conf.storage.cassandra.keyspace", KEYSPACE_NAME);
        c.set("titan.hadoop.sideeffect.format", "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat");
        c.set("titan.hadoop.output.format", "com.thinkaurelius.titan.hadoop.formats.graphson.GraphSONOutputFormat");
        c.set("cassandra.input.partitioner.class", "org.apache.cassandra.dht.Murmur3Partitioner");

        HadoopGraph hg = new HadoopGraph(c);

        assertEquals(0, new HadoopPipeline(hg).V().map().submit());
    }
}
