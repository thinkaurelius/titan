package com.thinkaurelius.titan.pkgtest;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.testcategory.OrderedKeyStoreTests;
import com.thinkaurelius.titan.testcategory.UnorderedKeyStoreTests;

public class FaunusCassandraIT extends AbstractTitanAssemblyIT {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Before
    public void deleteCassandraData() throws BackendException {
        StandardTitanGraph g = (StandardTitanGraph)TitanFactory.open("cassandrathrift:127.0.0.1");
        g.getBackend().clearStorage();
        // shutdown not necessary
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testGraphOfTheGodsWithBOP() throws Exception {
        unzipAndRunExpect("faunus-cassandra.expect.vm", ImmutableMap.of("cassandraPartitioner", "org.apache.cassandra.dht.ByteOrderedPartitioner"));
    }

    @Test
    @Category({ UnorderedKeyStoreTests.class })
    public void testGraphOfTheGodsWithMurmur() throws Exception {
        unzipAndRunExpect("faunus-cassandra.expect.vm", ImmutableMap.of("cassandraPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner"));
    }

    @Test
    @Category({ UnorderedKeyStoreTests.class })
    public void testGraphOfTheGodsWithVMRestart() throws Exception {
        unzipAndRunExpect("faunus-cassandra-vm-restart.expect.vm", ImmutableMap.of("cassandraPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner"));
    }
}
