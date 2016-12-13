package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractInputFormatIT extends TitanGraphBaseTest {

    @Before
    public void setup() throws IOException {
        FileUtils.deleteDirectory(new File("output"));
    }

    @Test
    public void testReadGraphOfTheGods() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        assertEquals(12L, (long) graph.traversal().V().count().next());
        Graph g = getGraph();
        GraphTraversalSource t = g.traversal(GraphTraversalSource.computer(SparkGraphComputer.class));
        assertEquals(12L, (long) t.V().count().next());
    }

    @Test
    public void testReadWideVertexWithManyProperties() throws Exception {
        int numProps = 1 << 16;

        long numV  = 1;
        mgmt.makePropertyKey("p").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.commit();
        finishSchema();

        for (int j = 0; j < numV; j++) {
            Vertex v = graph.addVertex();
            for (int i = 0; i < numProps; i++) {
                v.property("p", i);
            }
        }
        graph.tx().commit();

        assertEquals(numV, (long) graph.traversal().V().count().next());
        Map<String, Object> propertiesOnVertex = graph.traversal().V().valueMap().next();
        List<?> valuesOnP = (List)propertiesOnVertex.values().iterator().next();
        assertEquals(numProps, valuesOnP.size());
        Graph g = getGraph(); 
        GraphTraversalSource t = g.traversal(GraphTraversalSource.computer(SparkGraphComputer.class));
        assertEquals(numV, (long) t.V().count().next());
        propertiesOnVertex = t.V().valueMap().next();
        valuesOnP = (List)propertiesOnVertex.values().iterator().next();
        assertEquals(numProps, valuesOnP.size());
    }

    @Test
    public void testReadSelfEdge() throws Exception {
        GraphOfTheGodsFactory.load(graph, null, true);
        assertEquals(12L, (long) graph.traversal().V().count().next());

        // Add a self-loop on sky with edge label "lives"; it's nonsense, but at least it needs no schema changes
        TitanVertex sky = (TitanVertex)graph.query().has("name", "sky").vertices().iterator().next();
        assertNotNull(sky);
        assertEquals("sky", sky.value("name"));
        assertEquals(1L, sky.query().direction(Direction.IN).edgeCount());
        assertEquals(0L, sky.query().direction(Direction.OUT).edgeCount());
        assertEquals(1L, sky.query().direction(Direction.BOTH).edgeCount());
        sky.addEdge("lives", sky, "reason", "testReadSelfEdge");
        assertEquals(2L, sky.query().direction(Direction.IN).edgeCount());
        assertEquals(1L, sky.query().direction(Direction.OUT).edgeCount());
        assertEquals(3L, sky.query().direction(Direction.BOTH).edgeCount());
        graph.tx().commit();

        // Read the new edge using the inputformat
        Graph g = getGraph(); 
        GraphTraversalSource t = g.traversal(GraphTraversalSource.computer(SparkGraphComputer.class));
        Iterator<Object> edgeIdIter = t.V().has("name", "sky").bothE().id();
        assertNotNull(edgeIdIter);
        assertTrue(edgeIdIter.hasNext());
        Set<Object> edges = Sets.newHashSet(edgeIdIter);
        assertEquals(2, edges.size());
    }

    abstract protected Graph getGraph() throws IOException, ConfigurationException;
}
