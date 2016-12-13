package com.thinkaurelius.titan.diskstorage.solr.transform;

import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.diskstorage.BackendException;

public class GeoToWktConverter {
    /**
     * Get Well-Known Text format (e.g. POINT(X.0 Y.0)) for geoshape for indexing in solr.
     * Only points, lines and polygons are supported.
     */
    public static String convertToWktString(Geoshape fieldValue) throws BackendException {
        return fieldValue.toString();
    }
}
