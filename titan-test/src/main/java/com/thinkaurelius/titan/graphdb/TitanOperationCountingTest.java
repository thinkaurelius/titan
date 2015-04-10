package com.thinkaurelius.titan.graphdb;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.diskstorage.Backend;
import static com.thinkaurelius.titan.diskstorage.Backend.*;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.util.CacheMetricsAction;
import com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore;
import static com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore.*;


import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import static com.thinkaurelius.titan.graphdb.database.cache.MetricInstrumentedSchemaCache.*;
import static com.thinkaurelius.titan.testutil.TitanAssert.assertCount;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertexLabel;
import com.thinkaurelius.titan.graphdb.types.CompositeIndexType;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.testcategory.SerialTests;
import com.thinkaurelius.titan.util.stats.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Category({ SerialTests.class })
public abstract class TitanOperationCountingTest extends TitanGraphBaseTest {

    public MetricManager metric;
    public final String SYSTEM_METRICS  = GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;

    public abstract WriteConfiguration getBaseConfiguration();

    @Override
    public WriteConfiguration getConfiguration() {
        WriteConfiguration config = getBaseConfiguration();
        ModifiableConfiguration mconf = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE);
        mconf.set(BASIC_METRICS,true);
        mconf.set(METRICS_MERGE_STORES,false);
        mconf.set(PROPERTY_PREFETCHING,false);
        mconf.set(DB_CACHE,false);
        return config;
    }

    @Override
    public void open(WriteConfiguration config) {
        metric = MetricManager.INSTANCE;
        super.open(config);
    }

    @Test
    public void testIdCounts() {
        makeVertexIndexedUniqueKey("uid",Integer.class);
        mgmt.setConsistency(mgmt.getGraphIndex("uid"),ConsistencyModifier.LOCK);
        finishSchema();

        //Schema and relation id pools are tapped, Schema id pool twice because the renew is triggered. Each id acquisition requires 1 mutations and 2 reads
        verifyStoreMetrics(ID_STORE_NAME, SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 3l, M_GET_SLICE, 6l));
    }


    @Test
    public void testReadOperations() {
        testReadOperations(false);
    }

    @Test
    public void testReadOperationsWithCache() {
        testReadOperations(true);
    }

    public void testReadOperations(boolean cache) {
        metricsPrefix = "schema"+cache;

        resetEdgeCacheCounts();

        makeVertexIndexedUniqueKey("uid",Integer.class);
        mgmt.setConsistency(mgmt.getGraphIndex("uid"),ConsistencyModifier.LOCK);
        finishSchema();

        if (cache) clopen(option(DB_CACHE),true,option(DB_CACHE_CLEAN_WAIT),0,option(DB_CACHE_TIME),0);
        else clopen();

        TitanTransaction tx = graph.buildTransaction().groupName(metricsPrefix).start();
        tx.makePropertyKey("name").dataType(String.class).make();
        tx.makeEdgeLabel("knows").make();
        tx.makeVertexLabel("person").make();
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME);
        verifyStoreMetrics(INDEXSTORE_NAME, ImmutableMap.of(M_GET_SLICE, 3l, M_ACQUIRE_LOCK, 3l));

        resetMetrics();

        metricsPrefix=GraphDatabaseConfiguration.METRICS_SCHEMA_PREFIX_DEFAULT;

        resetMetrics();

        //Test schema caching
        for (int t=0;t<10;t++) {
            tx = graph.buildTransaction().groupName(metricsPrefix).start();
            //Retrieve name by index (one backend call each)
            assertTrue(tx.containsRelationType("name"));
            assertTrue(tx.containsRelationType("knows"));
            assertTrue(tx.containsVertexLabel("person"));
            PropertyKey name = tx.getPropertyKey("name");
            EdgeLabel knows = tx.getEdgeLabel("knows");
            VertexLabel person = tx.getVertexLabel("person");
            PropertyKey uid = tx.getPropertyKey("uid");
            //Retrieve name as property (one backend call each)
            assertEquals("name",name.name());
            assertEquals("knows",knows.name());
            assertEquals("person",person.name());
            assertEquals("uid",uid.name());
            //Looking up the definition (one backend call each)
            assertEquals(Cardinality.SINGLE,name.cardinality());
            assertEquals(Multiplicity.MULTI,knows.multiplicity());
            assertFalse(person.isPartitioned());
            assertEquals(Integer.class,uid.dataType());
            //Retrieving in and out relations for the relation types...
            InternalRelationType namei = (InternalRelationType)name;
            InternalRelationType knowsi = (InternalRelationType)knows;
            InternalRelationType uidi = (InternalRelationType)uid;
            assertNull(namei.getBaseType());
            assertNull(knowsi.getBaseType());
            IndexType index = Iterables.getOnlyElement(uidi.getKeyIndexes());
            assertEquals(1,index.getFieldKeys().length);
            assertEquals(ElementCategory.VERTEX,index.getElement());
            assertEquals(ConsistencyModifier.LOCK,((CompositeIndexType)index).getConsistencyModifier());
            assertEquals(1, Iterables.size(uidi.getRelationIndexes()));
            assertEquals(1, Iterables.size(namei.getRelationIndexes()));
            assertEquals(namei, Iterables.getOnlyElement(namei.getRelationIndexes()));
            assertEquals(knowsi, Iterables.getOnlyElement(knowsi.getRelationIndexes()));
            //.. and vertex labels
            assertEquals(0,((InternalVertexLabel)person).getTTL());

            tx.commit();
            //Needs to read on first iteration, after that it doesn't change anymore
            verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 19l));
            verifyStoreMetrics(INDEXSTORE_NAME,
                    ImmutableMap.of(M_GET_SLICE, 4l /* name, knows, person, uid */, M_ACQUIRE_LOCK, 0l));
        }

        //Create some graph data
        metricsPrefix = "add"+cache;

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        TitanVertex v = tx.addVertex(), u = tx.addVertex("person");
        v.property(VertexProperty.Cardinality.single, "uid",  1);
        u.property(VertexProperty.Cardinality.single, "name",  "juju");
        Edge e = v.addEdge("knows",u);
        e.property("name", "edge");
        tx.commit();
        verifyStoreMetrics(EDGESTORE_NAME);
        verifyStoreMetrics(INDEXSTORE_NAME, ImmutableMap.of(M_GET_SLICE, 1l, M_ACQUIRE_LOCK, 1l));

        for (int i = 1; i <= 20; i++) {
            metricsPrefix = "op"+i+cache;
            tx = graph.buildTransaction().groupName(metricsPrefix).start();
            v = getOnlyElement(tx.query().has("uid",1).vertices());
            assertEquals(1,v.<Integer>value("uid").intValue());
            u = getOnlyElement(v.query().labels("knows").vertices());
            e = getOnlyElement(u.query().direction(Direction.IN).labels("knows").edges());
            assertEquals("juju",u.value("name"));
            assertEquals("edge",e.value("name"));
            tx.commit();
            if (!cache || i==0) {
                verifyStoreMetrics(EDGESTORE_NAME, ImmutableMap.of(M_GET_SLICE, 4l));
                verifyStoreMetrics(INDEXSTORE_NAME, ImmutableMap.of(M_GET_SLICE, 1l));
            } else if (cache && i>10) { //Needs a couple of iterations for cache to be cleaned
                verifyStoreMetrics(EDGESTORE_NAME);
                verifyStoreMetrics(INDEXSTORE_NAME);
            }

        }


    }


    public static final List<String> STORE_NAMES =
            ImmutableList.of("edgeStore", "vertexIndexStore", "edgeIndexStore", "idStore");

    @Test
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void testKCVSAccess1() throws InterruptedException {
        metricsPrefix = "metrics1";

        TitanTransaction tx = graph.buildTransaction().groupName(metricsPrefix).start();
        TitanVertex v = tx.addVertex("age", 25, "name", "john");
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 2l, M_GET_SLICE, 4l));
        TitanVertex u = tx.addVertex("age", 35, "name", "mary");
        v.addEdge("knows", u);
        tx.commit();
//        printAllMetrics();
//        printAllMetrics(SYSTEM_METRICS);
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        Thread.sleep(500);
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(3, 3, 0, 0);

        //Check type name & definition caching
        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        assertCount(2, v.properties());
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 4l)); //1 verify vertex existence, 1 for query, 1 for each of the 2 types (Definition)
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(3, 3, 2, 2);
        tx.commit();

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        assertCount(2, v.properties());
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 6l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(3, 3, 4, 2);
        tx.commit();

        //Check type index lookup caching
        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        assertNotNull(v.value("age"));
        assertNotNull(v.value("name"));
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 11l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 5l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(9, 5, 8, 4);
        tx.commit();

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        assertCount(1, v.query().direction(Direction.BOTH).edges());
        assertCount(2, v.properties());
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 15l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 5l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyStoreMetrics(STORE_NAMES.get(2));
        verifyStoreMetrics(STORE_NAMES.get(3), SYSTEM_METRICS, ImmutableMap.of(M_MUTATE, 4l, M_GET_SLICE, 8l));
        verifyTypeCacheMetrics(9, 5, 11, 5);
        tx.commit();
    }

    @Test
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void testKCVSAccess2() throws InterruptedException {
        metricsPrefix = "metrics2";

        TitanTransaction tx = graph.buildTransaction().groupName(metricsPrefix).start();
        TitanVertex parentVertex = tx.addVertex();
        parentVertex.property(VertexProperty.Cardinality.single, "name",  "vParent");
        parentVertex.property(VertexProperty.Cardinality.single, "other-prop-key1",  "other-prop-value1");
        parentVertex.property(VertexProperty.Cardinality.single, "other-prop-key2",  "other-prop-value2");

        TitanVertex parentVertex2 = tx.addVertex();
        parentVertex2.property(VertexProperty.Cardinality.single, "name",  "vParent2");
        parentVertex2.property(VertexProperty.Cardinality.single, "other-prop-key1",  "other-prop-value12");
        parentVertex2.property(VertexProperty.Cardinality.single, "other-prop-key2",  "other-prop-value22");

        tx.commit();
        verifyStoreMetrics("edgeStore", ImmutableMap.of(M_MUTATE, 8l));
        verifyStoreMetrics("vertexIndexStore", ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyTypeCacheMetrics(3, 3, 0, 0);
        //==> 3 lookups in vertexIndex to see if types already exist, then 6 mutations (3+3 for lock) and 3 lock applications to create them
        //==> 8 mutations in edgeStore to create vertices and types
        //3 cache misses when doing the index lookup for the type names (since they are not yet defined)

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        assertCount(3, getV(tx,parentVertex).properties());
        tx.commit();
        verifyStoreMetrics("edgeStore", ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 5l));
        verifyStoreMetrics("vertexIndexStore", ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyTypeCacheMetrics(3, 3, 3, 3);
        //==> 5 edgeStore.getSlice (1 for vertex existence, 1 to retrieve all relations, 1 call per type (name+definition) for all 3 types)
        //==> of those, the 3 type related calls go through the cache which is empty at this point ==> 3 (additional) misses
        //all other stats remain unchanged

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        assertCount(3, getV(tx,parentVertex).properties());
        verifyStoreMetrics("edgeStore", ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 7l));
        verifyStoreMetrics("vertexIndexStore", ImmutableMap.of(M_GET_SLICE, 3l, M_MUTATE, 6l, M_ACQUIRE_LOCK, 3l));
        verifyTypeCacheMetrics(3, 3, 6, 3);
        //==> 2 edgeStore.getSlice (1 for vertex existence, 1 to retrieve all relations)
        //==> of those, the 3 type related calls go through the cache which is loaded at this point ==> 3 cache hits, no misses
        //==> there are only 2 getSlice calls that hit the storage backend
        //all other stats remain unchanged
    }

    @Test
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void checkFastPropertyTrue() {
        checkFastPropertyAndLocking(true);
    }

    @Test
    @Ignore //TODO: Ignore for now until everything is stable - then do the counting
    public void checkFastPropertyFalse() {
        checkFastPropertyAndLocking(false);
    }


    public void checkFastPropertyAndLocking(boolean fastProperty) {
        PropertyKey uid = makeKey("uid",String.class);
        TitanGraphIndex index = mgmt.buildIndex("uid",Vertex.class).unique().addKey(uid).buildCompositeIndex();
        mgmt.setConsistency(index, ConsistencyModifier.LOCK);
        finishSchema();

        clopen(option(GraphDatabaseConfiguration.PROPERTY_PREFETCHING), fastProperty);
        metricsPrefix = "metrics3"+fastProperty;

        TitanTransaction tx = graph.buildTransaction().groupName(metricsPrefix).start();
        tx.makePropertyKey("name").dataType(String.class).make();
        tx.makePropertyKey("age").dataType(Integer.class).make();
        TitanVertex v = tx.addVertex("uid", "v1", "age", 25, "name", "john");
        tx.commit();
        verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 7l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 4l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 3l));
        verifyTypeCacheMetrics(0, 0, 0, 0);

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        v.property(VertexProperty.Cardinality.single, "age",  35);
        v.property(VertexProperty.Cardinality.single, "name",  "johnny");
        tx.commit();
        if (fastProperty)
            verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 7l));
        else
            verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 8l, M_GET_SLICE, 7l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 6l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));
        if (fastProperty)
            verifyTypeCacheMetrics(6, 2, 5, 5);
        else
            verifyTypeCacheMetrics(6, 2, 4, 4);

        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        v.property(VertexProperty.Cardinality.single, "age",  45);
        v.property(VertexProperty.Cardinality.single, "name",  "johnnie");
        tx.commit();
        if (fastProperty)
            verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 9l, M_GET_SLICE, 9l));
        else
            verifyStoreMetrics(STORE_NAMES.get(0), ImmutableMap.of(M_MUTATE, 9l, M_GET_SLICE, 10l));
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 6l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));
        if (fastProperty) {
            verifyTypeCacheMetrics(12, 2, 10, 5);
        } else {
            verifyTypeCacheMetrics(12, 2, 8, 4);
        }

        //Check no further locks on read all
        tx = graph.buildTransaction().groupName(metricsPrefix).start();
        v = getV(tx,v);
        for (VertexProperty p : v.query().properties()) {
            assertNotNull(p.value());
            assertNotNull(p.key());
        }
        tx.commit();
        verifyStoreMetrics(STORE_NAMES.get(1), ImmutableMap.of(M_GET_SLICE, 6l, M_MUTATE, 7l, M_ACQUIRE_LOCK, 4l));

    }

    private String metricsPrefix;

    public void verifyStoreMetrics(String storeName) {
        verifyStoreMetrics(storeName, new HashMap<String, Long>(0));
    }

    public void verifyStoreMetrics(String storeName, Map<String, Long> operationCounts) {
        verifyStoreMetrics(storeName, metricsPrefix, operationCounts);
    }

    public void verifyStoreMetrics(String storeName, String prefix, Map<String, Long> operationCounts) {
        for (String operation : OPERATION_NAMES) {
            Long count = operationCounts.get(operation);
            if (count==null) count = 0l;
            assertEquals(Joiner.on(".").join(prefix, storeName, operation, MetricInstrumentedStore.M_CALLS),count.longValue(), metric.getCounter(prefix, storeName, operation, MetricInstrumentedStore.M_CALLS).getCount());
        }
    }

    public void verifyTypeCacheMetrics(int nameRetrievals, int nameMisses, int relationRetrievals, int relationMisses) {
        verifyTypeCacheMetrics(metricsPrefix,nameRetrievals,nameMisses,relationRetrievals,relationMisses);
    }

    public void verifyTypeCacheMetrics(String prefix, int nameRetrievals, int nameMisses, int relationRetrievals, int relationMisses) {
        assertEquals("On type cache name retrievals",nameRetrievals, metric.getCounter(prefix, METRICS_NAME, METRICS_TYPENAME, CacheMetricsAction.RETRIEVAL.getName()).getCount());
        assertEquals("On type cache name misses",nameMisses, metric.getCounter(prefix, METRICS_NAME, METRICS_TYPENAME, CacheMetricsAction.MISS.getName()).getCount());
        assertEquals("On type cache relation retrievals",relationRetrievals, metric.getCounter(prefix, METRICS_NAME, METRICS_RELATIONS, CacheMetricsAction.RETRIEVAL.getName()).getCount());
        assertEquals("On type cache relation misses", relationMisses, metric.getCounter(prefix, METRICS_NAME, METRICS_RELATIONS, CacheMetricsAction.MISS.getName()).getCount());
    }

//    public void verifyCacheMetrics(String storeName) {
//        verifyCacheMetrics(storeName,0,0);
//    }
//
//    public void verifyCacheMetrics(String storeName, int misses, int retrievals) {
//        verifyCacheMetrics(storeName, metricsPrefix, misses, retrievals);
//    }
//
//    public void verifyCacheMetrics(String storeName, String prefix, int misses, int retrievals) {
//        assertEquals("On "+storeName+"-cache retrievals",retrievals, metric.getCounter(prefix, storeName + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.RETRIEVAL.getName()).getCount());
//        assertEquals("On "+storeName+"-cache misses",misses, metric.getCounter(prefix, storeName + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.MISS.getName()).getCount());
//    }

    public void printAllMetrics() {
        printAllMetrics(metricsPrefix);
    }

    public void printAllMetrics(String prefix) {
        for (String store : STORE_NAMES) {
            System.out.println("######## Store: " + store + " (" + prefix + ")");
            for (String operation : MetricInstrumentedStore.OPERATION_NAMES) {
                System.out.println("-- Operation: " + operation);
                System.out.print("\t"); System.out.println(metric.getCounter(prefix, store, operation, MetricInstrumentedStore.M_CALLS).getCount());
                System.out.print("\t"); System.out.println(metric.getTimer(prefix, store, operation, MetricInstrumentedStore.M_TIME).getMeanRate());
                if (operation==MetricInstrumentedStore.M_GET_SLICE) {
                    System.out.print("\t"); System.out.println(metric.getCounter(prefix, store, operation, MetricInstrumentedStore.M_ENTRIES_COUNT).getCount());
                }
            }
        }
    }

    @Test
    @Ignore //TODO: Until things are stable
    public void testCacheConcurrency() throws InterruptedException {
        metricsPrefix = "evgt1";
        Object[] newConfig = {option(GraphDatabaseConfiguration.DB_CACHE),true,
                option(GraphDatabaseConfiguration.DB_CACHE_TIME),0,
                option(GraphDatabaseConfiguration.DB_CACHE_CLEAN_WAIT),0,
                option(GraphDatabaseConfiguration.DB_CACHE_SIZE),0.25,
                option(GraphDatabaseConfiguration.BASIC_METRICS),true,
                option(GraphDatabaseConfiguration.METRICS_MERGE_STORES),false,
                option(GraphDatabaseConfiguration.METRICS_PREFIX),metricsPrefix};
        clopen(newConfig);
        final String prop = "property";
        makeKey(prop,Integer.class);
        finishSchema();

        final int numV = 100;
        final long[] vids = new long[numV];
        for (int i=0;i<numV;i++) {
            TitanVertex v = graph.addVertex(prop,0);
            graph.tx().commit();
            vids[i]=getId(v);
        }
        clopen(newConfig);
        resetEdgeCacheCounts();

        final AtomicBoolean[] precommit = new AtomicBoolean[numV];
        final AtomicBoolean[] postcommit = new AtomicBoolean[numV];
        for (int i=0;i<numV;i++) {
            precommit[i]=new AtomicBoolean(false);
            postcommit[i]=new AtomicBoolean(false);
        }
        final AtomicInteger lookups = new AtomicInteger(0);
        final Random random = new Random();
        final int updateSleepTime = 40;
        final int readSleepTime = 2;
        final int numReads = Math.round((numV*updateSleepTime)/readSleepTime*2.0f);

        Thread reader = new Thread(new Runnable() {
            @Override
            public void run() {
                int reads = 0;
                while (reads<numReads) {
                    final int pos = random.nextInt(vids.length);
                    long vid = vids[pos];
                    TitanVertex v = getV(graph,vid);
                    assertNotNull(v);
                    boolean postCommit = postcommit[pos].get();
                    Integer value = v.value(prop);
                    lookups.incrementAndGet();
                    assertNotNull("On pos [" + pos + "]", value);
                    if (!precommit[pos].get()) assertEquals(0, value.intValue());
                    else if (postCommit) assertEquals(1, value.intValue());
                    graph.tx().commit();
                    try {
                        Thread.sleep(readSleepTime);
                    } catch (InterruptedException e) {
                        return;
                    }
                    reads++;
                }
            }
        });
        reader.start();

        Thread updater = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i=0;i<numV;i++) {
                    try {
                        TitanVertex v = getV(graph,vids[i]);
                        v.property(VertexProperty.Cardinality.single, prop, 1);
                        precommit[i].set(true);
                        graph.tx().commit();
                        postcommit[i].set(true);
                        Thread.sleep(updateSleepTime);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Unexpected interruption",e);
                    }
                }
            }
        });
        updater.start();
        updater.join();
//        reader.start();
        reader.join();

        System.out.println("Retrievals: " + getEdgeCacheRetrievals());
        System.out.println("Hits: " + (getEdgeCacheRetrievals()-getEdgeCacheMisses()));
        System.out.println("Misses: " + getEdgeCacheMisses());
        assertEquals(numReads, lookups.get());
        assertEquals(2 * numReads + 2 * numV + 2, getEdgeCacheRetrievals());
        int minMisses = 4*numV+2;
        assertTrue("Min misses ["+minMisses+"] vs actual ["+getEdgeCacheMisses()+"]",minMisses<=getEdgeCacheMisses() && 4*minMisses>=getEdgeCacheMisses());
    }

    private long getEdgeCacheRetrievals() {
        return MetricManager.INSTANCE.getCounter(metricsPrefix, "edgeStore" + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.RETRIEVAL.getName()).getCount();
    }

    private long getEdgeCacheMisses() {
        return MetricManager.INSTANCE.getCounter(metricsPrefix, "edgeStore" + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.MISS.getName()).getCount();
    }

    private void resetEdgeCacheCounts() {
        Counter counter = MetricManager.INSTANCE.getCounter(metricsPrefix, "edgeStore" + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.RETRIEVAL.getName());
        counter.dec(counter.getCount());
        counter = MetricManager.INSTANCE.getCounter(metricsPrefix, "edgeStore" + Backend.METRICS_CACHE_SUFFIX, CacheMetricsAction.MISS.getName());
        counter.dec(counter.getCount());
    }

    private void resetMetrics() {
        MetricManager.INSTANCE.getRegistry().removeMatching(MetricFilter.ALL);
    }

    //#################### MOVE REMAINING TO BENCHMARK ####################


    /**
     * Tests cache performance
     * TODO: move to benchmarks
     */
    @Test
    public void testCacheSpeedup() {
        Object[] newConfig = {option(GraphDatabaseConfiguration.DB_CACHE),true,
                option(GraphDatabaseConfiguration.DB_CACHE_TIME),0};
        clopen(newConfig);

        int numV = 1000;

        TitanVertex previous = null;
        for (int i=0;i<numV;i++) {
            TitanVertex v = graph.addVertex("name", "v" + i);
            if (previous!=null)
                v.addEdge("knows",previous);
            previous = v;
        }
        graph.tx().commit();
        long vertexId = getId(previous);
        assertCount(numV, graph.query().vertices());

        clopen(newConfig);

        double timecoldglobal=0, timewarmglobal=0,timehotglobal=0;

        int outerRepeat = 20;
        int measurements = 10;
        assertTrue(measurements<outerRepeat);
        int innerRepeat = 2;
        for (int c=0;c<outerRepeat;c++) {

            double timecold = testAllVertices(vertexId,numV);

            double timewarm = 0;
            double timehot = 0;
            for (int i = 0;i<innerRepeat;i++) {
                graph.tx().commit();
                timewarm += testAllVertices(vertexId,numV);
                for (int j=0;j<innerRepeat;j++) {
                    timehot += testAllVertices(vertexId,numV);
                }
            }
            timewarm = timewarm / innerRepeat;
            timehot = timehot / (innerRepeat*innerRepeat);

            if (c>=(outerRepeat-measurements)) {
                timecoldglobal += timecold;
                timewarmglobal += timewarm;
                timehotglobal  += timehot;
            }
//            System.out.println(timecold + "\t" + timewarm + "\t" + timehot);
            clopen(newConfig);
        }
        timecoldglobal = timecoldglobal/measurements;
        timewarmglobal = timewarmglobal/measurements;
        timehotglobal = timehotglobal/measurements;

        System.out.println(round(timecoldglobal) + "\t" + round(timewarmglobal) + "\t" + round(timehotglobal));
        assertTrue(timecoldglobal + " vs " + timewarmglobal, timecoldglobal>timewarmglobal*2);
        //assertTrue(timewarmglobal + " vs " + timehotglobal, timewarmglobal>timehotglobal); Sometimes, this is not true
    }

    private double testAllVertices(long vid, int numV) {
        long start = System.nanoTime();
        TitanVertex v = getV(graph,vid);
        for (int i=1; i<numV; i++) {
            v = getOnlyElement(v.query().direction(Direction.OUT).labels("knows").vertices());
        }
        return ((System.nanoTime()-start)/1000000.0);
    }

}
