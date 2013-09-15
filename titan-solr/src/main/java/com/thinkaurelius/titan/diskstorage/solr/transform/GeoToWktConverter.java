package com.thinkaurelius.titan.diskstorage.solr.transform;


import com.thinkaurelius.titan.core.attribute.Geoshape;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GeoToWktConverter {
    private static final String POINT_PATTERN = "((point)\\[([-+]?[0-9]*\\.?[0-9]+)(\\,\\s*)([-+]?[0-9]*\\.?[0-9]+)\\])";
    /**
     * {@link com.thinkaurelius.titan.core.attribute.Geoshape} stores Points in the String format: point[X.0,Y.0].
     * Solr needs it to be in Well-Known Text format: POINT(X.0 Y.0)
     */
    public static String convertToWktPoint(String titanPoint) {
        if (false == isTitanPoint(titanPoint)) {
            return "";
        }

        Pattern pattern = Pattern.compile(POINT_PATTERN);
        Matcher matcher = pattern.matcher(titanPoint);
        String wktString = titanPoint;
        while (matcher.find()) {
            //group 2 should say "point"
            //group 3 should be the X coordinate (lattitude).
            // We'll need to move this to the second since WKT does Long Lat
            String latitude = matcher.group(3);
            //group 4 should have the comma (and possibly whitespace) in it
            String delimiter = matcher.group(4);
            //group 5 should be the Y coordinate (longitude).
            //We'll need to swap its position too
            String longitude = matcher.group(5);
            wktString = "POINT(" + longitude + " " + latitude + ")";
        }
        return wktString;
    }

    public static boolean isTitanPoint(String titanGeoString) {
        Pattern pattern = Pattern.compile(POINT_PATTERN);
        Matcher matcher = pattern.matcher(titanGeoString);

        boolean found = false;
        while (matcher.find()) {
            //group 2 should say "point"
            String pointString = matcher.group(2);
            if (false == StringUtils.isBlank(pointString)) {
                found = true;
            }
        }
        return found;
    }

    public static boolean isGeoshape(Object fieldValue) {
        if (fieldValue instanceof Geoshape) {
            return true;
        }

        return false;
    }

    public static String convertToWktString(Object fieldValue) {
        String wkt = "";
        if (isTitanPoint(fieldValue.toString())) {
            wkt = convertToWktPoint(fieldValue.toString());
        }

        return wkt;
    }
}
