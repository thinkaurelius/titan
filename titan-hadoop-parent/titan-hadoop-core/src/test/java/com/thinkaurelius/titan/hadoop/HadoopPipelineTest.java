package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.hadoop.formats.cassandra.TitanCassandraOutputFormat;
import com.thinkaurelius.titan.hadoop.formats.edgelist.rdf.RDFInputFormat;
import com.thinkaurelius.titan.hadoop.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.titan.hadoop.formats.graphson.GraphSONOutputFormat;
import com.tinkerpop.pipes.transform.TransformPipe;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopPipelineTest extends BaseTest {

    public void testElementTypeUpdating() {
        HadoopPipeline pipe = new HadoopPipeline(new HadoopGraph());
        try {
            pipe.outE();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
        pipe.v(1, 2, 3, 4).outE("knows").inV().property("key");
        pipe = new HadoopPipeline(new HadoopGraph());
        pipe.V().E().V().E();


        try {
            pipe.V().inV();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

        try {
            pipe.E().outE();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

        try {
            pipe.E().outE();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testGraphSONToSequenceFile() throws Exception {
        Configuration c = new Configuration();

        c.set("titan.hadoop.input.format", GraphSONInputFormat.class.getName());
        c.set("titan.hadoop.input.location", "target/test-classes/com/thinkaurelius/titan/hadoop/formats/graphson/graph-of-the-gods.json");

        c.set("titan.hadoop.output.format", SequenceFileOutputFormat.class.getName());
        c.set("titan.hadoop.sideeffect.format", TextOutputFormat.class.getName());

        c.set("titan.hadoop.pipeline.track-paths", "true");
        c.set("titan.hadoop.pipeline.track-state", "true");

        assertEquals(0, new HadoopPipeline(new HadoopGraph(c))._().submit());
    }

    public void testGraphSONCustomOutputLocation() throws Exception {
        Configuration c = new Configuration();

        c.set("titan.hadoop.input.format", GraphSONInputFormat.class.getName());
        c.set("titan.hadoop.input.location", "target/test-classes/com/thinkaurelius/titan/hadoop/formats/graphson/graph-of-the-gods.json");

        File customOutputDir = new File("target/testGraphSONCustomOutputLocation");
        FileUtils.deleteQuietly(customOutputDir);
        assertFalse(customOutputDir.exists());

        c.set("titan.hadoop.output.format", GraphSONOutputFormat.class.getName());
        c.set("titan.hadoop.output.location", customOutputDir.getPath());
        c.set("titan.hadoop.sideeffect.format", TextOutputFormat.class.getName());

        assertEquals(0, new HadoopPipeline(new HadoopGraph(c))._().submit());
        assertTrue(customOutputDir.exists());
    }

    public void testRDFToGraphSON() throws Exception {
        Configuration c = new Configuration();

        c.set("titan.hadoop.input.format", RDFInputFormat.class.getName());
        c.set("titan.hadoop.input.location", "target/test-classes/com/thinkaurelius/titan/hadoop/formats/edgelist/rdf/graph-example-1.ntriple");

        c.set("titan.hadoop.output.format", GraphSONOutputFormat.class.getName());
        c.set("titan.hadoop.sideeffect.format", TextOutputFormat.class.getName());

        c.set("titan.hadoop.input.conf.format", "N_TRIPLES");
        c.set("titan.hadoop.input.conf.as-properties", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        c.set("titan.hadoop.input.conf.use-localname", "true");
        c.set("titan.hadoop.input.conf.literal-as-property", "true");

        assertEquals(0, new HadoopPipeline(new HadoopGraph(c))._().submit());
    }

    public void testPipelineLocking() {
        HadoopPipeline pipe = new HadoopPipeline(new HadoopGraph());
        pipe.V().out().property("name");

        try {
            pipe.V();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

        try {
            pipe.order(TransformPipe.Order.INCR, "name").V();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testPipelineLockingWithMapReduceOutput() throws Exception {
        HadoopGraph graph = new HadoopGraph();
        graph.setGraphOutputFormat(TitanCassandraOutputFormat.class);
        HadoopPipeline pipe = new HadoopPipeline(graph);
        assertFalse(pipe.state.isLocked());
        try {
            pipe.V().out().count().submit();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testPipelineStepIncr() {
        HadoopPipeline pipe = new HadoopPipeline(new HadoopGraph());
        assertEquals(pipe.state.getStep(), -1);
        pipe.V();
        assertEquals(pipe.state.getStep(), 0);
        pipe.as("a");
        assertEquals(pipe.state.getStep(), 0);
        pipe.has("name", "marko");
        assertEquals(pipe.state.getStep(), 0);
        pipe.out("knows");
        assertEquals(pipe.state.getStep(), 1);
        pipe.as("b");
        assertEquals(pipe.state.getStep(), 1);
        pipe.outE("battled");
        assertEquals(pipe.state.getStep(), 2);
        pipe.as("c");
        assertEquals(pipe.state.getStep(), 2);
        pipe.inV();
        assertEquals(pipe.state.getStep(), 3);
        pipe.as("d");
        assertEquals(pipe.state.getStep(), 3);

        assertEquals(pipe.state.getStep("a"), 0);
        assertEquals(pipe.state.getStep("b"), 1);
        assertEquals(pipe.state.getStep("c"), 2);
        assertEquals(pipe.state.getStep("d"), 3);
    }

}
