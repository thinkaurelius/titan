package com.thinkaurelius.titan.graphdb.embedded;

import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.AbstractCassandraGraphTest;
import com.thinkaurelius.titan.testcategory.ByteOrderedPartitionerTests;

@Category({ByteOrderedPartitionerTests.class})
public class InternalCassandraEmbeddedGraphTest extends AbstractCassandraGraphTest {

    public InternalCassandraEmbeddedGraphTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(InternalCassandraEmbeddedGraphTest.class.getSimpleName()));
    }

}
