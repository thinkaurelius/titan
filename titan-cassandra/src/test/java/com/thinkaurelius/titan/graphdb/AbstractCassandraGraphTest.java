package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.Iterables;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractCassandraGraphTest extends TitanGraphTest {

    public AbstractCassandraGraphTest(Configuration config) {
        super(config);
    }

    @Test
    public void testSetTimestamp() {
        // Transaction 1: Init graph with two vertices, having set "name" and "age" properties
        TitanTransaction tx1 = graph.newTransaction(100);
        String name = "name";
        String age = "age";
        String address = "address";

        Vertex v1 = tx1.addVertex();
        Vertex v2 = tx1.addVertex();
        v1.setProperty(name, "a");
        v2.setProperty(age, "14");
        v2.setProperty(name, "b");
        v2.setProperty(age, "42");
        tx1.commit();

        // Fetch vertex ids
        Object id1 = v1.getId();
        Object id2 = v2.getId();

        // Transaction 2: Remove "name" property from v1, set "address" property; create
        // an edge v2 -> v1
        TitanTransaction tx2 = graph.newTransaction(1000);
        v1 = tx2.getVertex(id1);
        v2 = tx2.getVertex(id2);
        v1.removeProperty(name);
        v1.setProperty(address, "xyz");
        Edge edge = tx2.addEdge(1, v2, v1, "parent");
        tx2.commit();
        Object edgeId = edge.getId();

        Vertex afterTx2 = graph.getVertex(id1);

        // Verify that "name" property is gone
        Assert.assertFalse(afterTx2.getPropertyKeys().contains(name));
        // Verify that "address" property is set
        Assert.assertEquals("xyz", afterTx2.getProperty(address));
        // Verify that the edge is properly registered with the endpoint vertex
        Assert.assertEquals(1, Iterables.size(afterTx2.getEdges(Direction.IN, "parent")));
        // Verify that edge is registered under the id
        Assert.assertNotNull(graph.getEdge(edgeId));
        graph.commit();

        // Transaction 3: Remove "address" property from v1 with earlier timestamp than
        // when the value was set
        TitanTransaction tx3 = graph.newTransaction(200);
        v1 = tx3.getVertex(id1);
        v1.removeProperty(address);
        tx3.commit();

        Vertex afterTx3 = graph.getVertex(id1);
        graph.commit();
        // Verify that "address" is still set
        Assert.assertEquals("xyz", afterTx3.getProperty(address));

        // Transaction 4: Modify "age" property on v2, remove edge between v2 and v1
        TitanTransaction tx4 = graph.newTransaction(2000);
        v2 = tx4.getVertex(id2);
        v2.setProperty(age, "15");
        tx4.removeEdge(tx4.getEdge(edgeId));
        tx4.commit();

        Vertex afterTx4 = graph.getVertex(id2);
        // Verify that "age" property is modified
        Assert.assertEquals("15", afterTx4.getProperty(age));
        // Verify that edge is no longer registered with the endpoint vertex
        Assert.assertEquals(0, Iterables.size(afterTx4.getEdges(Direction.OUT, "parent")));
        // Verify that edge entry disappeared from id registry
        Assert.assertNull(graph.getEdge(edgeId));

        // Transaction 5: Modify "age" property on v2 with earlier timestamp
        TitanTransaction tx5 = graph.newTransaction(1500);
        v2 = tx5.getVertex(id2);
        v2.setProperty(age, "16");
        tx5.commit();
        Vertex afterTx5 = graph.getVertex(id2);

        // Verify that the property value is unchanged
        Assert.assertEquals("15", afterTx5.getProperty(age));

    }
}