package com.thinkaurelius.titan.hadoop.serialize;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Geoshape.GeoshapeBinarySerializer;

/**
 * Register Titan classes requiring custom Kryo serialization for Spark.
 *
 */
public class TitanKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(Geoshape.class, new GeoShapeKryoSerializer());
    }

    /**
     * Geoshape serializer for Kryo.
     */
    public static class GeoShapeKryoSerializer extends Serializer<Geoshape> {
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

}
