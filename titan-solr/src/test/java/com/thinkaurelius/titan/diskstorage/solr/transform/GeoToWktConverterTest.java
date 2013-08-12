package com.thinkaurelius.titan.diskstorage.solr.transform;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Jared Holmberg (jholmberg@bericotechnologies.com)
 */
public class GeoToWktConverterTest {

    /**
     * Titan Geoshapes are converted to a string that gets sent to its respective index. Unfortunately, the string format
     * is not compatible with Solr 4. The GeoToWktConverter transforms the Geoshape's string value into a Well-Known Text
     * format understood by Solr.
     */
    @Test
    public void testConvertGeoshapePointToWktString() {
        String p1 = "point[35.4,48.9]"; //no spaces, no negative values
        String p2 = "point[35.4, 48.9]"; //spaces added
        String p3 = "point[-35.4,48.9]"; //negative longitude value
        String p4 = "point[35.4, -48.9]"; //negative latitude value

        assertTrue(GeoToWktConverter.isTitanPoint(p1));
        assertTrue(GeoToWktConverter.isTitanPoint(p2));
        assertTrue(GeoToWktConverter.isTitanPoint(p3));
        assertTrue(GeoToWktConverter.isTitanPoint(p4));

        String wkt1 = "POINT(35.4 48.9)";
        String wkt2 = "POINT(35.4 48.9)";
        String wkt3 = "POINT(-35.4 48.9)";
        String wkt4 = "POINT(35.4 -48.9)";
        assertEquals(wkt1, GeoToWktConverter.convertToWktPoint(p1));
        assertEquals(wkt2, GeoToWktConverter.convertToWktPoint(p2));
        assertEquals(wkt3, GeoToWktConverter.convertToWktPoint(p3));
        assertEquals(wkt4, GeoToWktConverter.convertToWktPoint(p4));

    }
}
