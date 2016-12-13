package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.context.jts.DatelineRule;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContextFactory;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.io.jts.JtsBinaryCodec;
import com.spatial4j.core.io.jts.JtsGeoJSONReader;
import com.spatial4j.core.io.jts.JtsGeoJSONWriter;
import com.spatial4j.core.io.jts.JtsWKTReader;
import com.spatial4j.core.io.jts.JtsWKTWriter;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

import org.apache.commons.io.output.ByteArrayOutputStream;

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A generic representation of a geographic shape, which can either be a single point,
 * circle, box, line or polygon. Use {@link #getType()} to determine the type of shape of a particular Geoshape object.
 * Use the static constructor methods to create the desired geoshape.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Geoshape {

    private static String FIELD_LABEL = "geometry";
    private static String FIELD_TYPE = "type";
    private static String FIELD_COORDINATES = "coordinates";
    private static String FIELD_RADIUS = "radius";

    public static final JtsSpatialContext CTX;
    static {
        JtsSpatialContextFactory factory = new JtsSpatialContextFactory();
        factory.geo = true;
        factory.useJtsPoint = false;
        factory.useJtsLineString = true;
        // TODO: Use default dateline rule and update to support multiline/polygon to resolve wrapping issues
        factory.datelineRule = DatelineRule.none;
        CTX = new JtsSpatialContext(factory);
    }

    private static JtsWKTReader wktReader = new JtsWKTReader(CTX, new JtsSpatialContextFactory());

    private static JtsWKTWriter wktWriter = new JtsWKTWriter(CTX, new JtsSpatialContextFactory());

    private static JtsGeoJSONReader geojsonReader = new JtsGeoJSONReader(CTX, new SpatialContextFactory());

    private static JtsGeoJSONWriter geojsonWriter = new JtsGeoJSONWriter(CTX, new SpatialContextFactory());

    /**
     * The Type of a shape: a point, box, circle, line or polygon.
     */
    public enum Type {
        POINT("Point"),
        BOX("Box"),
        CIRCLE("Circle"),
        LINE("Line"),
        POLYGON("Polygon"),
        MULTIPOINT("MultiPoint"),
        MULTILINESTRING("MultiLineString"),
        MULTIPOLYGON("MultiPolygon");

        private final String gsonName;

        Type(String gsonName) {
            this.gsonName = gsonName;
        }

        public boolean gsonEquals(String otherGson) {
            return otherGson != null && gsonName.equals(otherGson);
        }

        public static Type fromGson(String gsonShape) {
            return Type.valueOf(gsonShape.toUpperCase());
        }

        @Override
        public String toString() {
            return gsonName;
        }
    }

    private final Shape shape;

    private Geoshape(final Shape shape) {
        Preconditions.checkNotNull(shape,"Invalid shape (null)");
        this.shape = shape;
    }

    @Override
    public int hashCode() {
        return shape.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        Geoshape oth = (Geoshape)other;
        return shape.equals(oth.shape);
    }

    /**
     * Returns the WKT representation of the shape.
     * @return
     */
    @Override
    public String toString() {
        return wktWriter.toString(shape);
    }

    /**
     * Returns the GeoJSON representation of the shape.
     * @return
     */
    public String toGeoJson() {
        return GeoshapeGsonSerializer.toGeoJson(this);
    }

    /**
     * Returns the underlying {@link Shape}.
     * @return
     */
    public Shape getShape() {
        return shape;
    }

    /**
     * Returns the {@link Type} of this geoshape.
     *
     * @return
     */
    public Type getType() {
        final Type type;
        if (com.spatial4j.core.shape.Point.class.isAssignableFrom(shape.getClass())) {
            type = Type.POINT;
        } else if (Circle.class.isAssignableFrom(shape.getClass())) {
            type = Type.CIRCLE;
        } else if (Rectangle.class.isAssignableFrom(shape.getClass())) {
            type = Type.BOX;
        } else if (JtsGeometry.class.isAssignableFrom(shape.getClass())
                && "LineString".equals(((JtsGeometry) shape).getGeom().getGeometryType())) {
            type = Type.LINE;
        } else if (JtsGeometry.class.isAssignableFrom(shape.getClass())) {
            try {
                type = Type.fromGson((((JtsGeometry) shape).getGeom().getGeometryType()));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Unrecognized shape type");
            }
        } else {
            throw new IllegalStateException("Unrecognized shape type");
        }
        return type;
    }

    /**
     * Returns the number of points comprising this geoshape. A point and circle have only one point (center of cricle),
     * a box has two points (the south-west and north-east corners). Lines and polygons have a variable number of points.
     *
     * @return
     */
    public int size() {
        switch(getType()) {
            case POINT: return 1;
            case CIRCLE: return 1;
            case BOX: return 2;
            case LINE:
            case POLYGON: return ((JtsGeometry) shape).getGeom().getCoordinates().length;
            default: throw new IllegalStateException("size() not supported for type: " + getType());
        }
    }

    /**
     * Returns the point at the given position. The position must be smaller than {@link #size()}.
     *
     * @param position
     * @return
     */
    public Point getPoint(int position) {
        if (position<0 || position>=size()) throw new ArrayIndexOutOfBoundsException("Invalid position: " + position);
        switch(getType()) {
            case POINT:
            case CIRCLE:
                return getPoint();
            case BOX:
                if (position == 0)
                    return new Point(shape.getBoundingBox().getMinY(), shape.getBoundingBox().getMinX());
                else
                    return new Point(shape.getBoundingBox().getMaxY(), shape.getBoundingBox().getMaxX());
            case LINE:
            case POLYGON:
                Coordinate coordinate = ((JtsGeometry) shape).getGeom().getCoordinates()[position];
                return new Point(coordinate.y, coordinate.x);
            default:
                throw new IllegalStateException("getPoint(int) not supported for type: " + getType());
        }
    }

    /**
     * Returns the singleton point of this shape. Only applicable for point and circle shapes.
     *
     * @return
     */
    public Point getPoint() {
        Preconditions.checkArgument(getType()==Type.POINT || getType()==Type.CIRCLE,"Shape does not have a single point");
        return new Point(shape.getCenter().getY(), shape.getCenter().getX());
    }

    /**
     * Returns the radius in kilometers of this circle. Only applicable to circle shapes.
     * @return
     */
    public double getRadius() {
        Preconditions.checkArgument(getType()==Type.CIRCLE,"This shape is not a circle");
        double radiusInDeg = ((Circle) shape).getRadius();
        return DistanceUtils.degrees2Dist(radiusInDeg, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    }

    private SpatialRelation getSpatialRelation(Geoshape other) {
        Preconditions.checkNotNull(other);
        return shape.relate(other.shape);
    }

    /**
     * Whether this geometry has any points in common with the given geometry.
     * @param other
     * @return
     */
    public boolean intersect(Geoshape other) {
        SpatialRelation r = getSpatialRelation(other);
        return r==SpatialRelation.INTERSECTS || r==SpatialRelation.CONTAINS || r==SpatialRelation.WITHIN;
    }

    /**
     * Whether this geometry is within the given geometry.
     * @param outer
     * @return
     */
    public boolean within(Geoshape outer) {
        return getSpatialRelation(outer)==SpatialRelation.WITHIN;
    }

    /**
     * Whether this geometry contains the given geometry.
     * @param outer
     * @return
     */
    public boolean contains(Geoshape outer) {
        return getSpatialRelation(outer)==SpatialRelation.CONTAINS;
    }

    /**
     * Whether this geometry has no points in common with the given geometry.
     * @param other
     * @return
     */
    public boolean disjoint(Geoshape other) {
        return getSpatialRelation(other)==SpatialRelation.DISJOINT;
    }


    /**
     * Constructs a point from its latitude and longitude information
     * @param latitude
     * @param longitude
     * @return
     */
    public static final Geoshape point(final double latitude, final double longitude) {
        Preconditions.checkArgument(isValidCoordinate(latitude,longitude),"Invalid coordinate provided");
        return new Geoshape(CTX.makePoint(longitude,  latitude));
    }

    /**
     * Constructs a circle from a given center point and a radius in kilometer
     * @param latitude
     * @param longitude
     * @param radiusInKM
     * @return
     */
    public static final Geoshape circle(final double latitude, final double longitude, final double radiusInKM) {
        Preconditions.checkArgument(isValidCoordinate(latitude,longitude),"Invalid coordinate provided");
        Preconditions.checkArgument(radiusInKM>0,"Invalid radius provided [%s]",radiusInKM);
        double radius = DistanceUtils.dist2Degrees(radiusInKM, DistanceUtils.EARTH_MEAN_RADIUS_KM);
        return new Geoshape(CTX.makeCircle(longitude, latitude, radius));
    }

    /**
     * Constructs a new box shape which is identified by its south-west and north-east corner points
     * @param southWestLatitude
     * @param southWestLongitude
     * @param northEastLatitude
     * @param northEastLongitude
     * @return
     */
    public static final Geoshape box(final double southWestLatitude, final double southWestLongitude,
                                     final double northEastLatitude, final double northEastLongitude) {
        Preconditions.checkArgument(isValidCoordinate(southWestLatitude,southWestLongitude),"Invalid south-west coordinate provided");
        Preconditions.checkArgument(isValidCoordinate(northEastLatitude,northEastLongitude),"Invalid north-east coordinate provided");
        return new Geoshape(CTX.makeRectangle(southWestLongitude, northEastLongitude, southWestLatitude, northEastLatitude));
    }

    /**
     * Constructs a new line shape which is identified by its coordinates
     * @param coordinates Sequence of coordinates (lat1,lon1,...,latN,lonN)
     * @return
     */
    public static final Geoshape line(final double ... coordinates) {
        Preconditions.checkArgument(coordinates.length % 2 == 0, "Odd number of coordinates provided");
        Preconditions.checkArgument(coordinates.length >= 4, "Too few coordinate pairs provided");
        List<double[]> points = new ArrayList<>();
        for (int i = 0; i < coordinates.length; i+=2) {
            points.add(new double[] {coordinates[i+1], coordinates[i]});
        }
        return line(points);
    }

    /**
     * Constructs a line from list of coordinates
     * @param coordinates Coordinate (lon,lat) pairs
     * @return
     */
    public static final Geoshape line(List<double[]> coordinates) {
        Preconditions.checkArgument(coordinates.size() >= 2, "Too few coordinate pairs provided");
        List<com.spatial4j.core.shape.Point> points = new ArrayList<>();
        for (double[] coordinate : coordinates) {
            Preconditions.checkArgument(isValidCoordinate(coordinate[1],coordinate[0]),"Invalid coordinate provided");
            points.add(CTX.makePoint(coordinate[0],  coordinate[1]));
        }
        return new Geoshape(CTX.makeLineString(points));
    }

    /**
     * Constructs a new polygon shape which is identified by its coordinates
     * @param coordinates Sequence of coordinates (lat1,lon1,...,latN,lonN)
     * @return
     */
    public static final Geoshape polygon(final double ... coordinates) {
        Preconditions.checkArgument(coordinates.length % 2 == 0, "Odd number of coordinates provided");
        Preconditions.checkArgument(coordinates.length >= 6, "Too few coordinate pairs provided");
        List<double[]> points = new ArrayList<>();
        for (int i = 0; i < coordinates.length; i+=2) {
            points.add(new double[] {coordinates[i+1], coordinates[i]});
        }
        return polygon(points);
    }

    /**
     * Constructs a polygon from list of coordinates
     * @param coordinates Coordinate (lon,lat) pairs
     * @return
     */
    public static final Geoshape polygon(List<double[]> coordinates) {
        Preconditions.checkArgument(coordinates.size() >= 3, "Too few coordinate pairs provided");
        Coordinate[] points = new Coordinate[coordinates.size()];
        for (int i=0; i<coordinates.size(); i++) {
            Preconditions.checkArgument(coordinates.get(i).length >= 2, "Too few coordinates in pair");
            Preconditions.checkArgument(isValidCoordinate(coordinates.get(i)[1],coordinates.get(i)[0]),"Invalid coordinate provided");
            points[i] = new Coordinate(coordinates.get(i)[0], coordinates.get(i)[1]);
        }

        Polygon polygon = new GeometryFactory().createPolygon(points);
        return new Geoshape(CTX.makeShape(polygon));
    }

    /**
     * Constructs a Geoshape from a JTS {@link Geometry}.
     * @param geometry
     * @return
     */
    public static final Geoshape geoshape(Geometry geometry) {
        return new Geoshape(CTX.makeShape(geometry));
    }

    /**
     * Constructs a Geoshape from a spatial4j {@link Shape}.
     * @param shape
     * @return
     */
    public static final Geoshape geoshape(Shape shape) {
        return new Geoshape(shape);
    }

    /**
     * Create Geoshape from WKT representation.
     * @param wkt
     * @return
     * @throws ParseException
     */
    public static final Geoshape fromWkt(String wkt) throws ParseException {
        return new Geoshape(wktReader.parse(wkt));
    }

    /**
     * Whether the given coordinates mark a point on earth.
     * @param latitude
     * @param longitude
     * @return
     */
    public static final boolean isValidCoordinate(final double latitude, final double longitude) {
        return latitude>=-90.0 && latitude<=90.0 && longitude>=-180.0 && longitude<=180.0;
    }

    /**
     * A single point representation. A point is identified by its coordinate on the earth sphere using the spherical
     * system of latitudes and longitudes.
     */
    public static final class Point {

        private final double longitude;
        private final double latitude;

        /**
         * Constructs a point with the given latitude and longitude
         * @param latitude Between -90 and 90 degrees
         * @param longitude Between -180 and 180 degrees
         */
        Point(double latitude, double longitude) {
            this.longitude = longitude;
            this.latitude = latitude;
        }

        /**
         * Longitude of this point
         * @return
         */
        public double getLongitude() {
            return longitude;
        }

        /**
         * Latitude of this point
         * @return
         */
        public double getLatitude() {
            return latitude;
        }

        private com.spatial4j.core.shape.Point getSpatial4jPoint() {
            return CTX.makePoint(longitude,latitude);
        }

        /**
         * Returns the distance to another point in kilometers
         *
         * @param other Point
         * @return
         */
        public double distance(Point other) {
            return DistanceUtils.degrees2Dist(CTX.getDistCalc().distance(getSpatial4jPoint(),other.getSpatial4jPoint()),DistanceUtils.EARTH_MEAN_RADIUS_KM);
        }

    }

    /**
     * Geoshape attribute serializer for Titan.
     * @author Matthias Broecheler (me@matthiasb.com)
     */
    public static class GeoshapeSerializer implements AttributeSerializer<Geoshape> {

        @Override
        public void verifyAttribute(Geoshape value) {
            //All values of Geoshape are valid
        }

        @Override
        public Geoshape convert(Object value) {

            if(value instanceof Map) {
                return convertGeoJson(value);
            }

            if(value instanceof Collection) {
                value = convertCollection((Collection<Object>) value);
            }

            if (value.getClass().isArray() && (value.getClass().getComponentType().isPrimitive() ||
                    Number.class.isAssignableFrom(value.getClass().getComponentType())) ) {
                Geoshape shape = null;
                int len= Array.getLength(value);
                double[] arr = new double[len];
                for (int i=0;i<len;i++) arr[i]=((Number)Array.get(value,i)).doubleValue();
                if (len==2) shape= point(arr[0],arr[1]);
                else if (len==3) shape= circle(arr[0],arr[1],arr[2]);
                else if (len==4) shape= box(arr[0],arr[1],arr[2],arr[3]);
                else throw new IllegalArgumentException("Expected 2-4 coordinates to create Geoshape, but given: " + value);
                return shape;
            } else if (value instanceof String) {
                String[] components=null;
                for (String delimiter : new String[]{",",";"}) {
                    components = ((String)value).split(delimiter);
                    if (components.length>=2 && components.length<=4) break;
                    else components=null;
                }
                Preconditions.checkArgument(components!=null,"Could not parse coordinates from string: %s",value);
                double[] coords = new double[components.length];
                try {
                    for (int i=0;i<components.length;i++) {
                        coords[i]=Double.parseDouble(components[i]);
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Could not parse coordinates from string: " + value, e);
                }
                return convert(coords);
            } else return null;
        }


        private double[] convertCollection(Collection<Object> c) {

            List<Double> numbers = c.stream().map(o -> {
                if (!(o instanceof Number)) {
                    throw new IllegalArgumentException("Collections may only contain numbers to create a Geoshape");
                }
                return ((Number) o).doubleValue();
            }).collect(Collectors.toList());
            return Doubles.toArray(numbers);
        }

        private Geoshape convertGeoJson(Object value) {
            //Note that geoJson is long,lat
            try {
                Map<String, Object> map = (Map) value;
                String type = (String) map.get("type");
                if("Feature".equals(type)) {
                    Map<String, Object> geometry = (Map) map.get("geometry");
                    return convertGeometry(geometry);
                } else {
                    return convertGeometry(map);
                }
            } catch (ClassCastException | IOException | ParseException e) {
                throw new IllegalArgumentException("GeoJSON was unparsable");
            }
        }

        private Geoshape convertGeometry(Map<String, Object> geometry) throws IOException, ParseException {
            String type = (String) geometry.get("type");
            List<Object> coordinates = (List) geometry.get("coordinates");

            if ("Point".equals(type)) {
                double[] parsedCoordinates = convertCollection(coordinates);
                return point(parsedCoordinates[1], parsedCoordinates[0]);
            } else if ("Circle".equals(type)) {
                Number radius = (Number) geometry.get("radius");
                if (radius == null) {
                    throw new IllegalArgumentException("GeoJSON circles require a radius");
                }
                double[] parsedCoordinates = convertCollection(coordinates);
                return circle(parsedCoordinates[1], parsedCoordinates[0], radius.doubleValue());
            } else if ("Polygon".equals(type)) {
                // check whether this is a box
                if (coordinates.size() == 4) {
                    double[] p0 = convertCollection((Collection) coordinates.get(0));
                    double[] p1 = convertCollection((Collection) coordinates.get(1));
                    double[] p2 = convertCollection((Collection) coordinates.get(2));
                    double[] p3 = convertCollection((Collection) coordinates.get(3));

                    //This may be a clockwise or counterclockwise polygon, we have to verify that it is a box
                    if ((p0[0] == p1[0] && p1[1] == p2[1] && p2[0] == p3[0] && p3[1] == p0[1] && p3[0] != p0[0]) ||
                            (p0[1] == p1[1] && p1[0] == p2[0] && p2[1] == p3[1] && p3[0] == p0[0] && p3[1] != p0[1])) {
                        return box(min(p0[1], p1[1], p2[1], p3[1]), min(p0[0], p1[0], p2[0], p3[0]), max(p0[1], p1[1], p2[1], p3[1]), max(p0[0], p1[0], p2[0], p3[0]));
                    }
                }
            }

            String json = new ObjectMapper().writeValueAsString(geometry);
            return new Geoshape(geojsonReader.read(new StringReader(json)));
        }

        private double min(double... numbers) {
            return Arrays.stream(numbers).min().getAsDouble();
        }

        private double max(double... numbers) {
            return Arrays.stream(numbers).max().getAsDouble();
        }


        @Override
        public Geoshape read(ScanBuffer buffer) {
            long l = VariableLong.readPositive(buffer);
            assert l>0 && l<Integer.MAX_VALUE;
            int length = (int)l;
            InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(length));
            try {
                return GeoshapeBinarySerializer.read(inputStream);
            } catch (IOException e) {
                throw new RuntimeException("I/O exception reading geoshape");
            }
        }

        @Override
        public void write(WriteBuffer buffer, Geoshape attribute) {
            try {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                GeoshapeBinarySerializer.write(outputStream, attribute);
                byte[] bytes = outputStream.toByteArray();
                VariableLong.writePositive(buffer,bytes.length);
                buffer.putBytes(bytes);
            } catch (IOException e) {
                throw new RuntimeException("I/O exception writing geoshape");
            }
        }
    }

    /**
     * Geoshape serializer for TinkerPop's Gryo.
     */
    public static class GeoShapeGryoSerializer extends Serializer<Geoshape> {
        @Override
        public void write(Kryo kryo, Output output, Geoshape geoshape) {
            try {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                GeoshapeBinarySerializer.write(outputStream, geoshape);
                byte[] bytes = outputStream.toByteArray();
                output.write(bytes.length);
                output.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException("I/O exception writing geoshape");
            }
        }

        @Override
        public Geoshape read(Kryo kryo, Input input, Class<Geoshape> aClass) {
            int length = input.read();
            assert length>0;
            InputStream inputStream = new ByteArrayInputStream(input.readBytes(length));
            try {
                return GeoshapeBinarySerializer.read(inputStream);
            } catch (IOException e) {
                throw new RuntimeException("I/O exception reding geoshape");
            }
        }
    }

    /**
     * Geoshape serializer supports writing GeoJSON (http://geojson.org/).
     */
    public static class GeoshapeGsonSerializer extends StdSerializer<Geoshape> {

        public GeoshapeGsonSerializer() {
            super(Geoshape.class);
        }

        @Override
        public void serialize(Geoshape value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            switch(value.getType()) {
                case POINT:
                    jgen.writeStartObject();
                    jgen.writeFieldName(FIELD_TYPE);
                    jgen.writeString(Type.POINT.toString());
                    jgen.writeFieldName(FIELD_COORDINATES);
                    jgen.writeStartArray();
                    jgen.writeNumber(value.getPoint().getLongitude());
                    jgen.writeNumber(value.getPoint().getLatitude());
                    jgen.writeEndArray();
                    jgen.writeEndObject();
                    break;
                default:
                    jgen.writeRawValue(toGeoJson(value));
                    break;
            }
        }

        @Override
        public void serializeWithType(Geoshape geoshape, JsonGenerator jgen, SerializerProvider serializerProvider,
                                      TypeSerializer typeSerializer) throws IOException, JsonProcessingException {

            jgen.writeStartObject();
            if (typeSerializer != null) jgen.writeStringField(GraphSONTokens.CLASS, Geoshape.class.getName());
            String geojson = toGeoJson(geoshape);
            Map json = new ObjectMapper().readValue(geojson, LinkedHashMap.class);
            if (geoshape.getType() == Type.POINT) {
                double[] coords = ((List<Number>) json.get("coordinates")).stream().map(i -> i.doubleValue()).mapToDouble(i -> i).toArray();
                GraphSONUtil.writeWithType(FIELD_COORDINATES, coords, jgen, serializerProvider, typeSerializer);
            } else {
                GraphSONUtil.writeWithType(FIELD_LABEL, json, jgen, serializerProvider, typeSerializer);
            }
            jgen.writeEndObject();
        }

        public static String toGeoJson(Geoshape geoshape) {
            return geojsonWriter.toString(geoshape.shape);
        }

    }

    /**
     * Geoshape JSON deserializer supporting reading from GeoJSON (http://geojson.org/).
     */
    public static class GeoshapeGsonDeserializer extends StdDeserializer<Geoshape> {

        public GeoshapeGsonDeserializer() {
            super(Geoshape.class);
        }

        @Override
        public Geoshape deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            jsonParser.nextToken();
            if (jsonParser.getCurrentName().equals("coordinates")) {
                double[] f = jsonParser.readValueAs(double[].class);
                jsonParser.nextToken();
                return Geoshape.point(f[1], f[0]);
            } else {
                try {
                    HashMap map = jsonParser.readValueAs(LinkedHashMap.class);
                    jsonParser.nextToken();
                    String json = new ObjectMapper().writeValueAsString(map);
                    Geoshape shape = new Geoshape(geojsonReader.read(new StringReader(json)));
                    return shape;
                } catch (ParseException e) {
                    throw new IOException("Unable to read and parse geojson", e);
                }
            }
        }
    }

    /**
     * Geoshape binary serializer using spatial4j's {@link JtsBinaryCodec}.
     *
     */
    public static class GeoshapeBinarySerializer {

        private static JtsBinaryCodec binaryCodec = new JtsBinaryCodec(CTX, new JtsSpatialContextFactory());

        /**
         * Serialize a geoshape.
         * @param outputStream
         * @param attribute
         * @return
         * @throws IOException
         */
        public static void write(OutputStream outputStream, Geoshape attribute) throws IOException {
            outputStream.write(attribute.shape instanceof JtsGeometry ? 0 : 1);
            try (DataOutputStream dataOutput = new DataOutputStream(outputStream)) {
                if (attribute.shape instanceof JtsGeometry) {
                    binaryCodec.writeJtsGeom(dataOutput, attribute.shape);
                } else {
                    binaryCodec.writeShape(dataOutput, attribute.shape);
                }
                dataOutput.flush();
            }
            outputStream.flush();
        }

        /**
         * Deserialize a geoshape.
         * @param inputStream
         * @return
         * @throws IOException
         */
        public static Geoshape read(InputStream inputStream) throws IOException {
            boolean isJts = inputStream.read()==0;
            try (DataInputStream dataInput = new DataInputStream(inputStream)) {
                if (isJts) {
                    return new Geoshape(binaryCodec.readJtsGeom(dataInput));
                } else {
                    return new Geoshape(binaryCodec.readShape(dataInput));
                }
            }
        }
    }

}
