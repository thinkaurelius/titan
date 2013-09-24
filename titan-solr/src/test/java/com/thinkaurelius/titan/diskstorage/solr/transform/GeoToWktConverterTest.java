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

        String wkt1 = "POINT(48.9 35.4)";
        String actualWkt1 = GeoToWktConverter.convertToWktPoint(p1);

        String wkt2 = "POINT(48.9 35.4)";
        String actualWkt2 = GeoToWktConverter.convertToWktPoint(p2);

        String wkt3 = "POINT(48.9 -35.4)";
        String actualWkt3 = GeoToWktConverter.convertToWktPoint(p3);

        String wkt4 = "POINT(-48.9 35.4)";
        String actualWkt4 = GeoToWktConverter.convertToWktPoint(p4);

        assertEquals(wkt1, actualWkt1);
        assertEquals(wkt2, actualWkt2);
        assertEquals(wkt3, actualWkt3);
        assertEquals(wkt4, actualWkt4);

    }
}
