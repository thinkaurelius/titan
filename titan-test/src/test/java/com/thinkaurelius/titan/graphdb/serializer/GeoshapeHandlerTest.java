package com.thinkaurelius.titan.graphdb.serializer;

import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.GeoshapeHandler;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class GeoshapeHandlerTest {

    @Test
    public void testString() {
        GeoshapeHandler handler = new GeoshapeHandler();

        assertEquals(handler.convert("1,2"), Geoshape.point(1, 2));
        assertEquals(handler.convert("1,2,4"), Geoshape.circle(1, 2, 4));
        assertEquals(handler.convert("1,2,4,8"), Geoshape.box(1, 2, 4, 8));

        assertEquals(handler.convert("point[1,2]"), Geoshape.point(1, 2));
        assertEquals(handler.convert("circle[1,2,4]"), Geoshape.circle(1, 2, 4));
        assertEquals(handler.convert("box[1,2,4,8]"), Geoshape.box(1, 2, 4, 8));

        String[] strings = {
                "",
                "1",
                "1,2,4,8,16",
                "point[", "point[]", "point[1]", "point[1,2,4]",
                "circle[", "circle[1,2]", "circle[1,2,4,8]",
                "box[", "box[1,2]", "box[1,2,4]", "box[1,2,4,8,16]"
        };

        for(String string: strings) {
            try {
                handler.convert(string);
                fail("should have failed to parse: \"" + string + "\"");
            } catch (IllegalArgumentException e) {
            }
        }
    }

    @Test
    public void testArray() {
        GeoshapeHandler handler = new GeoshapeHandler();

        assertEquals(handler.convert(new double[] {1, 2}), Geoshape.point(1, 2));
        assertEquals(handler.convert(new double[] {1, 2, 4}), Geoshape.circle(1, 2, 4));
        assertEquals(handler.convert(new double[] {1, 2, 4, 8}), Geoshape.box(1, 2, 4, 8));

        double[][] arrays = {
                new double[] {1},
                new double[] {1, 2, 4, 8, 16}
        };

        for(double[] array: arrays) {
            try {
                handler.convert(array);
                fail("should have failed to parse: \"" + array + "\"");
            } catch (IllegalArgumentException e) {
            }
        }
    }

    @Test
    public void testList() {
        GeoshapeHandler handler = new GeoshapeHandler();

        assertEquals(handler.convert(Arrays.asList(1, 2)), Geoshape.point(1, 2));
        assertEquals(handler.convert(Arrays.asList(1, 2, 4)), Geoshape.circle(1, 2, 4));
        assertEquals(handler.convert(Arrays.asList(1, 2, 4, 8)), Geoshape.box(1, 2, 4, 8));

        List<List<Integer>> lists = Arrays.asList(
                Arrays.asList(1),
                Arrays.asList(1, 2, 4, 8, 16)
        );

        for(List<Integer> list: lists) {
            try {
                handler.convert(list);
                fail("should have failed to parse: \"" + lists + "\"");
            } catch (IllegalArgumentException e) {
            }
        }
    }
}
