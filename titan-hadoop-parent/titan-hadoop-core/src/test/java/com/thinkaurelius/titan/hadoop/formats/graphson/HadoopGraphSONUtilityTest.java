package com.thinkaurelius.titan.hadoop.formats.graphson;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Edge;

import junit.framework.TestCase;

import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopGraphSONUtilityTest extends TestCase {

    public void testParser1() throws IOException {
        HadoopGraphSONUtility util = new HadoopGraphSONUtility(new ModifiableHadoopConfiguration());
        FaunusVertex vertex = util.fromJSON("{\"_id\":1}");
        assertEquals(vertex.getLongId(), 1l);
        assertFalse(vertex.getEdges(OUT).iterator().hasNext());
    }

    public void testParser2() throws IOException {
        HadoopGraphSONUtility util = new HadoopGraphSONUtility(new ModifiableHadoopConfiguration());
        FaunusVertex vertex = util.fromJSON("{\"_id\":1, \"name\":\"marko\",\"age\":32}");
        assertEquals(vertex.getLongId(), 1l);
        assertFalse(vertex.getEdges(OUT).iterator().hasNext());
        assertFalse(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getProperty("age"), 32);
    }

    public void testParser3() throws IOException {
        HadoopGraphSONUtility util = new HadoopGraphSONUtility(new ModifiableHadoopConfiguration());
        FaunusVertex vertex = util.fromJSON("{\"_id\":1, \"name\":\"marko\",\"age\":32, \"_outE\":[{\"_inV\":2, \"_label\":\"knows\"}, {\"_inV\":3, \"_label\":\"created\"}]}");
        assertEquals(vertex.getLongId(), 1l);
        assertTrue(vertex.getEdges(OUT).iterator().hasNext());
        assertFalse(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getProperty("age"), 32);
        List<Edge> edges = BaseTest.asList(vertex.getEdges(OUT));
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("knows") || edge.getLabel().equals("created"));
        }
        assertEquals(edges.size(), 2);
    }

    public void testParser4() throws IOException {
        HadoopGraphSONUtility util = new HadoopGraphSONUtility(new ModifiableHadoopConfiguration());
        FaunusVertex vertex = util.fromJSON("{\"_id\":4, \"name\":\"josh\", \"age\":32, \"_outE\":[{\"_inV\":3, \"_label\":\"created\", \"weight\":0.4}, {\"_inV\":5, \"_label\":\"created\", \"weight\":1.0}], \"_inE\":[{\"_outV\":1, \"_label\":\"knows\", \"weight\":1.0}]}");
        assertEquals(vertex.getLongId(), 4l);
        assertTrue(vertex.getEdges(OUT).iterator().hasNext());
        assertTrue(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "josh");
        assertEquals(vertex.getProperty("age"), 32);
        List<Edge> edges = BaseTest.asList(vertex.getEdges(OUT));
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("created"));
        }
        assertEquals(edges.size(), 2);
        edges = BaseTest.asList(vertex.getEdges(IN));
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("knows"));
            assertEquals(edge.getProperty("weight"), 1);
        }
        assertEquals(edges.size(), 1);
    }

    public void testWriter1() throws Exception {
        FaunusVertex marko = new FaunusVertex(new ModifiableHadoopConfiguration(), 1l);
        marko.setProperty("name", "marko");
        FaunusVertex stephen = new FaunusVertex(new ModifiableHadoopConfiguration(), 2l);
        stephen.setProperty("name", "stephen");
        FaunusVertex vadas = new FaunusVertex(new ModifiableHadoopConfiguration(), 3l);
        vadas.setProperty("name", "vadas");

        marko.addEdge(OUT, new StandardFaunusEdge(new ModifiableHadoopConfiguration(), marko.getLongId(), stephen.getLongId(), "knows")).setProperty("weight", 2);
        marko.addEdge(IN, new StandardFaunusEdge(new ModifiableHadoopConfiguration(), vadas.getLongId(), marko.getLongId(), "knows")).setProperty("weight", 1);

        HadoopGraphSONUtility util = new HadoopGraphSONUtility(new ModifiableHadoopConfiguration());
        JSONObject m = util.toJSON(marko);
        JSONObject s = util.toJSON(stephen);

        assertEquals(m.getString("name"), "marko");
        assertEquals(m.getLong("_id"), 1l);
        assertFalse(m.has("_type"));
        assertEquals(m.getJSONArray("_outE").length(), 1);
        assertEquals(m.getJSONArray("_outE").getJSONObject(0).getLong("weight"), 2);
        assertFalse(m.getJSONArray("_outE").getJSONObject(0).has("_type"));
        assertFalse(m.getJSONArray("_outE").getJSONObject(0).has("_outV"));
        assertEquals("knows", m.getJSONArray("_outE").getJSONObject(0).optString("_label"));
        assertEquals(m.getJSONArray("_inE").length(), 1);
        assertEquals(m.getJSONArray("_inE").getJSONObject(0).getLong("weight"), 1);
        assertFalse(m.getJSONArray("_inE").getJSONObject(0).has("_type"));
        assertFalse(m.getJSONArray("_inE").getJSONObject(0).has("_inV"));
        assertEquals("knows", m.getJSONArray("_inE").getJSONObject(0).optString("_label"));

        assertEquals(s.getString("name"), "stephen");
        assertEquals(s.getLong("_id"), 2l);
        assertFalse(m.has("_type"));
        assertNull(s.optJSONArray("_outE"));
        assertNull(s.optJSONArray("_inE"));

    }
}
