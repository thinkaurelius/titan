package com.thinkaurelius.titan.hadoop.formats.cassandra;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.HadoopPipeline;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.TitanOutputFormatTest;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraOutputFormatTest extends TitanOutputFormatTest {

    @BeforeClass
    public static void startUpCassandra() throws Exception {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Test
    public void testWideRows() throws Exception {
        bulkLoadGraphOfTheGods(f1);
        clopen();
        Configuration c = new Configuration();
        final File customOutputDir = new File("target/jobs/job-0");
        assertFalse(customOutputDir.exists());
        c.set("titan.hadoop.input.format", "com.thinkaurelius.titan.hadoop.formats.cassandra.TitanCassandraInputFormat");
        c.set("titan.hadoop.input.conf.storage.backend", "cassandrathrift");
        String className = TitanCassandraOutputFormatTest.class.getSimpleName();
        c.set("titan.hadoop.input.conf.storage.cassandra.keyspace", CassandraStorageSetup.cleanKeyspaceName(className));
        c.set("titan.hadoop.sideeffect.format", "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat");
        c.set("titan.hadoop.output.format", "com.thinkaurelius.titan.hadoop.formats.graphson.GraphSONOutputFormat");
        c.set("titan.hadoop.output.location", customOutputDir.getPath());
        c.set("cassandra.input.partitioner.class", "org.apache.cassandra.dht.Murmur3Partitioner");
        c.set("cassandra.input.widerows", "true");

        HadoopGraph hg = new HadoopGraph(c);

        assertEquals(0, new HadoopPipeline(hg).V().map().submit());

        // Count the number of vertices emitted by HadoopPipeline reading from Cassandra and writing to textfiles
        assertTotalLineCountInJobFiles(12, customOutputDir.getPath(), "sideeffect-m-");
    }

    @Override
    protected void setCustomFaunusOptions(ModifiableHadoopConfiguration c) {
        c.getHadoopConfiguration().set(
                "cassandra.input.partitioner.class",
                "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    @Override
    protected ModifiableConfiguration getTitanConfiguration() {
        String className = TitanCassandraOutputFormatTest.class.getSimpleName();
        ModifiableConfiguration mc = CassandraStorageSetup.getCassandraThriftConfiguration(className);
        mc.set(STORAGE_HOSTS, new String[]{"localhost"});
        mc.set(DB_CACHE, false);
        mc.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, "tmp");
        return mc;
    }

    @Override
    protected Class<?> getTitanInputFormatClass() {
        return TitanCassandraInputFormat.class;
    }

    @Override
    protected Class<?> getTitanOutputFormatClass() {
        return TitanCassandraOutputFormat.class;
    }

    private void assertTotalLineCountInJobFiles(
            final int expectedLines, final String dir, final String filenamePrefix) throws IOException {

        final File outputDir = new File(dir);
        final FilenameFilter sideEffectFilenameFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(filenamePrefix);
            }
        };

        int lines = 0;

        for (File f : outputDir.listFiles(sideEffectFilenameFilter)) {
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            while (null != br.readLine())
                lines++;
            br.close();
        }

        assertEquals(expectedLines, lines);
    }
}
