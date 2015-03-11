package com.thinkaurelius.titan.olap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.olap.QueryContainer;
import com.thinkaurelius.titan.graphdb.olap.VertexJobConverter;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.graphdb.olap.job.GhostVertexRemover;
import com.tinkerpop.gremlin.process.computer.*;
import com.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class OLAPTest extends TitanGraphBaseTest {

    private static final double EPSILON = 0.00001;

    private static final Random random = new Random();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    private ScanMetrics executeScanJob(VertexScanJob job) throws Exception {
        return executeScanJob(VertexJobConverter.convert(graph,job));
    }

    private ScanMetrics executeScanJob(ScanJob job) throws Exception {
        return graph.getBackend().buildEdgeScanJob()
                .setNumProcessingThreads(2)
                .setJob(job)
                .execute().get();
    }

    private int generateRandomGraph(int numV) {
        mgmt.makePropertyKey("uid").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makePropertyKey("values").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.makePropertyKey("numvals").dataType(Integer.class).make();
        finishSchema();
        int numE = 0;
        Vertex[] vs = new Vertex[numV];
        for (int i=0;i<numV;i++) {
            vs[i] = tx.addVertex("uid",i+1);
            int numVals = random.nextInt(5)+1;
            vs[i].singleProperty("numvals",numVals);
            for (int j=0;j<numVals;j++) {
                vs[i].property("values",random.nextInt(100));
            }
        }
        for (int i=0;i<numV;i++) {
            int edges = i+1;
            Vertex v = vs[i];
            for (int j=0;j<edges;j++) {
                Vertex u = vs[random.nextInt(numV)];
                v.addEdge("knows", u);
                numE++;
            }
        }
        assertEquals(numV*(numV+1),numE*2);
        return numE;
    }

    @Test
    public void testVertexScan() throws Exception {
        int numV = 100;
        int numE = generateRandomGraph(numV);
        final String DEGREE_COUNT = "degree";
        final String VERTEX_COUNT = "numV";
        clopen();

        ScanMetrics result1 = executeScanJob(new VertexScanJob() {

            @Override
            public void process(TitanVertex vertex, ScanMetrics metrics) {
                long outDegree = vertex.outE("knows").count().next();
                assertEquals(0,vertex.inE("knows").count().next().longValue());
                assertEquals(1, vertex.properties("uid").count().next().longValue());
                assertTrue(vertex.<Integer>property("uid").orElse(0) > 0);
                metrics.incrementCustom(DEGREE_COUNT,outDegree);
                metrics.incrementCustom(VERTEX_COUNT);
            }

            @Override
            public void getQueries(QueryContainer queries) {
                queries.addQuery().labels("knows").direction(Direction.OUT).edges();
                queries.addQuery().keys("uid").properties();
            }
        });
        assertEquals(numV,result1.getCustom(VERTEX_COUNT));
        assertEquals(numE,result1.getCustom(DEGREE_COUNT));

        ScanMetrics result2 = executeScanJob(new VertexScanJob() {

            @Override
            public void process(TitanVertex vertex, ScanMetrics metrics) {
                metrics.incrementCustom(VERTEX_COUNT);
                assertEquals(1,vertex.properties("numvals").count().next().longValue());
                int numvals = vertex.value("numvals");
                assertEquals(numvals,vertex.properties("values").count().next().longValue());
            }

            @Override
            public void getQueries(QueryContainer queries) {
                queries.addQuery().keys("values").properties();
                queries.addQuery().keys("numvals").properties();
            }
        });
        assertEquals(numV,result2.getCustom(VERTEX_COUNT));
    }

    @Test
    public void removeGhostVertices() throws Exception {
        Vertex v1 = tx.addVertex("person");
        v1.property("name","stephen");
        Vertex v2 = tx.addVertex("person");
        v1.property("name","marko");
        Vertex v3 = tx.addVertex("person");
        v1.property("name","dan");
        v2.addEdge("knows",v3);
        v1.addEdge("knows",v2);
        newTx();
        long v3id = getId(v3);
        long v1id = getId(v1);
        assertTrue(v3id>0);

        v3 = getV(tx, v3id);
        assertNotNull(v3);
        v3.remove();
        tx.commit();

        TitanTransaction xx = graph.buildTransaction().checkExternalVertexExistence(false).start();
        v3 = getV(xx, v3id);
        assertNotNull(v3);
        v1 = getV(xx, v1id);
        assertNotNull(v1);
        v3.property("name","deleted");
        v3.addEdge("knows",v1);
        xx.commit();

        newTx();
        assertNull(getV(tx,v3id));
        v1 = getV(tx, v1id);
        assertNotNull(v1);
        assertEquals(v3id,v1.in("knows").next().id());
        tx.commit();
        mgmt.commit();

        ScanMetrics result = executeScanJob(new GhostVertexRemover(graph));
        assertEquals(1,result.getCustom(GhostVertexRemover.REMOVED_VERTEX_COUNT));
        assertEquals(2,result.getCustom(GhostVertexRemover.REMOVED_RELATION_COUNT));
        assertEquals(0,result.getCustom(GhostVertexRemover.SKIPPED_GHOST_LIMIT_COUNT));
    }

    @Test
    public void degreeCounting() throws Exception {
        int numV = 200;
        int numE = generateRandomGraph(numV);
        clopen();

        final TitanGraphComputer computer = graph.compute();
        computer.resultMode(TitanGraphComputer.ResultMode.NONE);
        computer.workers(4);
        computer.program(new DegreeCounter());
        computer.mapReduce(new DegreeMapper());
        ComputerResult result = computer.submit().get();
        System.out.println("Execution time (ms) ["+numV+"|"+numE+"]: " + result.memory().getRuntime());
        assertTrue(result.memory().exists(DegreeMapper.DEGREE_RESULT));
        Map<Long,Integer> degrees = result.memory().get(DegreeMapper.DEGREE_RESULT);
        assertNotNull(degrees);
        assertEquals(numV,degrees.size());
        int totalCount = 0;
        for (Map.Entry<Long,Integer> entry : degrees.entrySet()) {
            int degree = entry.getValue();
            Vertex v = getV(tx, entry.getKey().longValue());
            int count = v.value("uid");
            assertEquals(count,degree);
            totalCount+= degree;
        }
        assertEquals(numV*(numV+1)/2,totalCount);
        assertEquals(1,result.memory().getIteration());
    }

    @Test
    public void degreeCountingDistance() throws Exception {
        int numV = 100;
        int numE = generateRandomGraph(numV);
        clopen();

        for (TitanGraphComputer.ResultMode mode : TitanGraphComputer.ResultMode.values()) {
            final TitanGraphComputer computer = graph.compute();
            computer.resultMode(mode);
            computer.workers(1);
            computer.program(new DegreeCounter(2));
            ComputerResult result = computer.submit().get();
            System.out.println("Execution time (ms) ["+numV+"|"+numE+"]: " + result.memory().getRuntime());
            assertEquals(2,result.memory().getIteration());

            Graph gview = null;
            switch (mode) {
                case LOCALTX: gview = result.graph(); break;
                case PERSIST: newTx(); gview = tx; break;
                case NONE: break;
                default: throw new AssertionError(mode);
            }
            if (gview == null) continue;

            for (Vertex v : gview.V().toList()) {
                long degree2 = ((Integer)v.value(DegreeCounter.DEGREE)).longValue();
                long actualDegree2 = v.out().out().count().next();
                assertEquals(actualDegree2,degree2);
            }
            if (mode== TitanGraphComputer.ResultMode.LOCALTX) {
                assertTrue(gview instanceof TitanTransaction);
                ((TitanTransaction)gview).rollback();
            }
        }
    }

    public static class DegreeCounter extends StaticVertexProgram<Integer> {

        public static final String DEGREE = "degree";
        public static final MessageCombiner<Integer> ADDITION = (a,b) -> a+b;
        public static final MessageScope.Local<Integer> DEG_MSG = MessageScope.Local.of(AnonymousGraphTraversal.Tokens.__::inE);

        private final int length;

        DegreeCounter() {
            this(1);
        }

        DegreeCounter(int length) {
            Preconditions.checkArgument(length>0);
            this.length = length;
        }

        @Override
        public void setup(Memory memory) {
            return;
        }

        @Override
        public void execute(Vertex vertex, Messenger<Integer> messenger, Memory memory) {
            if (memory.isInitialIteration()) {
                messenger.sendMessage(DEG_MSG, 1);
            } else {
                int degree = StreamFactory.stream(messenger.receiveMessages(DEG_MSG)).reduce(0, (a, b) -> a + b);
                vertex.singleProperty(DEGREE, degree);
                if (memory.getIteration()<length) messenger.sendMessage(DEG_MSG, degree);
            }
        }

        @Override
        public boolean terminate(Memory memory) {
            return memory.getIteration()>=length;
        }

        @Override
        public Set<String> getElementComputeKeys() {
            return ImmutableSet.of(DEGREE);
        }

        @Override
        public Optional<MessageCombiner<Integer>> getMessageCombiner() {
            return Optional.of(ADDITION);
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            if (memory.getIteration()<length) return ImmutableSet.of((MessageScope)DEG_MSG);
            else return Collections.EMPTY_SET;
        }

        @Override
        public Features getFeatures() {
            return new Features() {
                @Override
                public boolean requiresLocalMessageScopes() {
                    return true;
                }

                @Override
                public boolean requiresVertexPropertyAddition() {
                    return true;
                }
            };
        }


    }

    public static class DegreeMapper extends StaticMapReduce<Long,Integer,Long,Integer,Map<Long,Integer>> {

        public static final String DEGREE_RESULT = "degrees";

        @Override
        public boolean doStage(Stage stage) {
            return stage==Stage.MAP;
        }

        @Override
        public void map(Vertex vertex, MapEmitter<Long, Integer> emitter) {
            emitter.emit((Long)vertex.id(),vertex.value(DegreeCounter.DEGREE));
        }

        @Override
        public Map<Long, Integer> generateFinalResult(Iterator<KeyValue<Long, Integer>> keyValues) {
            Map<Long,Integer> result = new HashMap<>();
            for (; keyValues.hasNext(); ) {
                KeyValue<Long, Integer> r =  keyValues.next();
                result.put(r.getKey(),r.getValue());
            }
            return result;
        }

        @Override
        public String getMemoryKey() {
            return DEGREE_RESULT;
        }

    }
}
