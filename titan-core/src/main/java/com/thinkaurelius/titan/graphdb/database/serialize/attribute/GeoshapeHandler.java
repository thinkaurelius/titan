package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.attribute.Geoshape;

import java.lang.reflect.Array;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class GeoshapeHandler implements AttributeHandler<Geoshape> {

    private static final String POINT_PREFIX = "point[";
    private static final String CIRCLE_PREFIX = "circle[";
    private static final String BOX_PREFIX = "box[";
    private static final String SUFFIX = "]";

    @Override
    public void verifyAttribute(Geoshape value) {
        //All values of Geoshape are valid
    }

    @Override
    public Geoshape convert(Object value) {
        if (value.getClass().isArray() && (value.getClass().getComponentType().isPrimitive() ||
                Number.class.isAssignableFrom(value.getClass().getComponentType())) ) {
            int len = Array.getLength(value);
            double[] arr = new double[len];
            for (int i=0;i<len;i++) {
                arr[i] = ((Number)Array.get(value, i)).doubleValue();
            }
            return convertArray(arr);
        } else if (value instanceof List) {
            List list = (List)value;
            double[] arr = new double[list.size()];

            int i = 0;
            for (Object coord: list) {
                Preconditions.checkArgument(coord != null && Number.class.isAssignableFrom(coord.getClass()),
                        "Could not parse coordinates from list: %s", value);
                arr[i] = ((Number)coord).doubleValue();
                i += 1;
            }

            return convertArray(arr);
        } else if (value instanceof String) {
            String string = (String)value;
            double[] coords;

            if (string.startsWith(POINT_PREFIX)) {
                Preconditions.checkArgument(string.endsWith(SUFFIX), "Could not parse coordinates from string: %s", value);
                coords = parseString(string.substring(POINT_PREFIX.length(), string.length() - 1));
                Preconditions.checkArgument(coords.length == 2, "Expected 2 components for box: %s", value);

            } else if (string.startsWith(CIRCLE_PREFIX)) {
                Preconditions.checkArgument(string.endsWith(SUFFIX), "Could not parse coordinates from string: %s", value);
                coords = parseString(string.substring(CIRCLE_PREFIX.length(), string.length() - 1));
                Preconditions.checkArgument(coords.length == 3, "Expected 3 components for circle: %s", value);

            } else if (string.startsWith(BOX_PREFIX)) {
                Preconditions.checkArgument(string.endsWith(SUFFIX), "Could not parse coordinates from string: %s", value);
                coords = parseString(string.substring(BOX_PREFIX.length(), string.length() - 1));
                Preconditions.checkArgument(coords.length == 4, "Expected 4 components for box: %s", value);

            } else {
                coords = parseString(string);
            }

            return convertArray(coords);
        } else return null;
    }

    private Geoshape convertArray(double[] arr) {
        switch (arr.length) {
            case 2: return Geoshape.point(arr[0], arr[1]);
            case 3: return Geoshape.circle(arr[0], arr[1], arr[2]);
            case 4: return Geoshape.box(arr[0], arr[1], arr[2], arr[3]);
            default: {
                throw new IllegalArgumentException("Expected 2-4 coordinates to create Geoshape, but given: " + arr);
            }
        }
    }

    private double[] parseString(String string) {
        String[] components = null;

        for (String delimiter : new String[]{",",";"}) {
            components = string.split(delimiter);
            if (components.length>=2 && components.length<=4) break;
            else components = null;
        }

        Preconditions.checkArgument(components != null, "Could not parse coordinates from string: %s", string);

        double[] coords = new double[components.length];
        try {
            for (int i = 0; i < components.length; i++) {
                coords[i]=Double.parseDouble(components[i]);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Could not parse coordinates from string: " + string, e);
        }

        return coords;
    }
}
