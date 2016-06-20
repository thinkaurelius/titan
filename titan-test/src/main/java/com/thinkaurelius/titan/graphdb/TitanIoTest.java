package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;

import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.function.Function;

/**
 * Tests Titan specific serialization classes not covered by the TinkerPop suite.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class TitanIoTest extends TitanGraphBaseTest {

    private GeometryFactory gf;

    @Before
    public void setup() {
        gf = new GeometryFactory();
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);
        TitanManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("shape").dataType(Geoshape.class).make();
        mgmt.commit();
    }

    @Test
    public void testSerializationReadWriteAsGraphSONEmbedded() throws Exception {
        testSerializationReadWriteAsGraphSONEmbedded(null);
        testSerializationReadWriteAsGraphSONEmbedded(makeLine);
        testSerializationReadWriteAsGraphSONEmbedded(makePoly);
        testSerializationReadWriteAsGraphSONEmbedded(makeMultiPoint);
        testSerializationReadWriteAsGraphSONEmbedded(makeMultiLine);
        testSerializationReadWriteAsGraphSONEmbedded(makeMultiPolygon);
    }

    @Test
    public void testSerializationReadWriteAsGryo() throws Exception {
        testSerializationReadWriteAsGryo(null);
        testSerializationReadWriteAsGryo(makeLine);
        testSerializationReadWriteAsGryo(makePoly);
        testSerializationReadWriteAsGryo(makeMultiPoint);
        testSerializationReadWriteAsGryo(makeMultiLine);
        testSerializationReadWriteAsGryo(makeMultiPolygon);
    }

    public void testSerializationReadWriteAsGraphSONEmbedded(Function<Geoshape,Geoshape> makeGeoshape) throws Exception {
        if (makeGeoshape != null) {
            addGeoshape(makeGeoshape);
        }
        GraphSONMapper m = graph.io(IoCore.graphson()).mapper().embedTypes(true).create();
        GraphWriter writer = graph.io(IoCore.graphson()).writer().mapper(m).create();
        FileOutputStream fos = new FileOutputStream("/tmp/test.json");
        writer.writeGraph(fos, graph);

        clearGraph(config);
        open(config);

        GraphReader reader = graph.io(IoCore.graphson()).reader().mapper(m).create();
        FileInputStream fis = new FileInputStream("/tmp/test.json");
        reader.readGraph(fis, graph);

        TitanIndexTest.assertGraphOfTheGods(graph);
        if (makeGeoshape != null) {
            assertGeoshape(makeGeoshape);
        }
    }

    private void testSerializationReadWriteAsGryo(Function<Geoshape,Geoshape> makeGeoshape) throws Exception {
        if (makeGeoshape != null) {
            addGeoshape(makeGeoshape);
        }
        graph.io(IoCore.gryo()).writeGraph("/tmp/test.kryo");

        clearGraph(config);
        open(config);

        graph.io(IoCore.gryo()).readGraph("/tmp/test.kryo");

        TitanIndexTest.assertGraphOfTheGods(graph);
        if (makeGeoshape != null) {
            assertGeoshape(makeGeoshape);
        }
    }

    private void addGeoshape(Function<Geoshape,Geoshape> makeGeoshape) {
        TitanTransaction tx = graph.newTransaction();
        graph.traversal().E().has("place").toList().stream().forEach(e-> {
            Geoshape place = (Geoshape) e.property("place").value();
            e.property("shape", makeGeoshape.apply(place));
        });
        tx.commit();
    }

    private void assertGeoshape(Function<Geoshape,Geoshape> makeGeoshape) {
        graph.traversal().E().has("place").toList().stream().forEach(e-> {
            assertTrue(e.property("shape").isPresent());
            Geoshape place = (Geoshape) e.property("place").value();
            Geoshape expected = makeGeoshape.apply(place);
            Geoshape actual = (Geoshape) e.property("shape").value();
            assertEquals(expected, actual);
        });
    }

    private Function<Geoshape,Geoshape> makePoly = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return Geoshape.polygon(x,y,x,y+1,x+1,y+1,x+1,y,x,y,x,y);
    };

    private Function<Geoshape,Geoshape> makeLine = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return Geoshape.line(x,y,x,y+1,x+1,y+1,x+1,y);
    };

    private Function<Geoshape,Geoshape> makeMultiPoint = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return Geoshape.geoshape(gf.createMultiPoint(new Coordinate[] {new Coordinate(x,y), new Coordinate(x+1,y+1)}));
    };

    private Function<Geoshape,Geoshape> makeMultiLine = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return Geoshape.geoshape(gf.createMultiLineString(new LineString[] {
                gf.createLineString(new Coordinate[] {new Coordinate(x,y), new Coordinate(x+1,y+1)}),
                gf.createLineString(new Coordinate[] {new Coordinate(x-1,y-1), new Coordinate(x,y)})}));
    };

    private Function<Geoshape,Geoshape> makeMultiPolygon = place -> {
        double x = Math.floor(place.getPoint().getLongitude());
        double y = Math.floor(place.getPoint().getLatitude());
        return Geoshape.geoshape(gf.createMultiPolygon(new Polygon[] {
                gf.createPolygon(new Coordinate[] {new Coordinate(x,y), new Coordinate(x+1,y), new Coordinate(x+1,y+1), new Coordinate(x,y)}),
                gf.createPolygon(new Coordinate[] {new Coordinate(x+2,y+2), new Coordinate(x+2,y+3), new Coordinate(x+3,y+3), new Coordinate(x+2,y+2)})}));
    };

}
