package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Namifiable;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.OrderPreservingSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SupportsNullSerializer;
import com.thinkaurelius.titan.util.encoding.StringEncoding;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Serializes Strings by trying to find the most efficient serialization format:
 * 1) ASCII encoding (one byte per char)
 * 2) Full UTF encoding (for non-ASCII strings)
 * 3) Using compression algorithms for long strings
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StringSerializer implements OrderPreservingSerializer<String>, SupportsNullSerializer {

    public static final int MAX_LENGTH = 128 * 1024 * 1024; //128 MB

    public static final int LONG_COMPRESSION_THRESHOLD = 16000;
    public static final int TEXT_COMRPESSION_THRESHOLD = 48;

    private static final long COMPRESSOR_BIT_LEN = 3;
    private static final int MAX_NUM_COMPRESSORS = (1<<COMPRESSOR_BIT_LEN);
    private static final long COMPRESSOR_BIT_MASK = MAX_NUM_COMPRESSORS-1;
    private static final long NO_COMPRESSION_OFFSET = COMPRESSOR_BIT_LEN+1;


    private final CharacterSerializer cs = new CharacterSerializer();

    @Override
    public String readByteOrder(ScanBuffer buffer) {
        byte prefix = buffer.getByte();
        if (prefix==-1) return null;
        assert prefix==0;
        StringBuilder s = new StringBuilder();
        while (true) {
            char c = cs.readByteOrder(buffer);
            if (((int) c) > 0) s.append(c);
            else break;
        }
        return s.toString();
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, String attribute) {
        if (attribute==null) {
            buffer.putByte((byte)-1);
            return;
        } else {
            buffer.putByte((byte)0);
        }
        for (int i = 0; i < attribute.length(); i++) {
            char c = attribute.charAt(i);
            Preconditions.checkArgument(((int) c) > 0, "No null characters allowed in string @ position %s: %s", i, attribute);
            cs.writeByteOrder(buffer, c);
        }
        cs.writeByteOrder(buffer, (char) 0);
    }

    @Override
    public void verifyAttribute(String value) {
        Preconditions.checkArgument(value.length()<=MAX_LENGTH,"String is too long: %s",value.length());
    }

    @Override
    public String convert(Object value) {
        Preconditions.checkNotNull(value);
        if (value instanceof String) return (String)value;
        else if (value instanceof Namifiable) return ((Namifiable)value).name();
        else return value.toString();
    }


    @Override
    public String read(ScanBuffer buffer) {
        long length = VariableLong.readPositive(buffer);
        if (length==0) return null;

        long compressionId = length & COMPRESSOR_BIT_MASK;
        assert compressionId<MAX_NUM_COMPRESSORS;
        CompressionType compression = CompressionType.getFromId((int)compressionId);
        length = (length>>>COMPRESSOR_BIT_LEN);
        String value;
        if (compression==CompressionType.NO_COMPRESSION) {
            if ( (length&1)==0) { //ASCII encoding
                length = length>>>1;
                if (length==1) value="";
                else if (length==2) {
                    StringBuilder sb = new StringBuilder();
                    while (true) {
                        int c = 0xFF & buffer.getByte();
                        sb.append((char)(c & 0x7F));
                        if ((c & 0x80) > 0) break;
                    }
                    value = sb.toString();
                } else throw new IllegalArgumentException("Invalid ASCII encoding offset: " + length);
            } else { //variable full UTF encoding
                length = length>>>1;
                assert length>0 && length<=Integer.MAX_VALUE;
                StringBuilder sb = new StringBuilder((int)length);
                for (int i = 0; i < length; i++) {
                    int b = buffer.getByte() & 0xFF;
                    switch (b >> 4) {
                        case 0:
                        case 1:
                        case 2:
                        case 3:
                        case 4:
                        case 5:
                        case 6:
                        case 7:
                            sb.append((char)b);
                            break;
                        case 12:
                        case 13:
                            sb.append((char)((b & 0x1F) << 6 | buffer.getByte() & 0x3F));
                            break;
                        case 14:
                            sb.append((char)((b & 0x0F) << 12 | (buffer.getByte() & 0x3F) << 6 | buffer.getByte() & 0x3F));
                            break;
                    }
                }
                value = sb.toString();
            }
        } else {
            assert length<=Integer.MAX_VALUE;
            value = compression.decompress(buffer,(int)length);
        }
        return value;
    }

    @Override
    public void write(WriteBuffer buffer, String attribute) {
        CompressionType compression;
        if (attribute==null) {
            VariableLong.writePositive(buffer,0);
            return;
        } else if (attribute.length()>LONG_COMPRESSION_THRESHOLD) {
            compression=CompressionType.GZIP;
        } else {
            compression=CompressionType.NO_COMPRESSION;
        }
        assert compression!=null;
        assert compression.getId()<MAX_NUM_COMPRESSORS;
        if (compression==CompressionType.NO_COMPRESSION) {
            assert compression.getId()==0;
            if (StringEncoding.isAsciiString(attribute)) {
                if (attribute.length()==0) VariableLong.writePositive(buffer,1l<<NO_COMPRESSION_OFFSET);
                else VariableLong.writePositive(buffer,2l<<NO_COMPRESSION_OFFSET);
                for (int i = 0; i < attribute.length(); i++) {
                    int c = attribute.charAt(i);
                    assert c <= 127;
                    byte b = (byte)c;
                    if (i+1==attribute.length()) b |= 0x80; //End marker
                    buffer.putByte(b);
                }
            } else {
                assert attribute.length()>0;
                VariableLong.writePositive(buffer,(((long)attribute.length())<<NO_COMPRESSION_OFFSET) + (1l<<COMPRESSOR_BIT_LEN)); //Marker for full UTF encoding
                for (int i = 0; i < attribute.length(); i++) { //variable encoding of the characters
                    int c = attribute.charAt(i);
                    if (c <= 0x007F) {
                        buffer.putByte((byte)c);
                    } else if (c > 0x07FF) {
                        buffer.putByte((byte)(0xE0 | c >> 12 & 0x0F));
                        buffer.putByte((byte)(0x80 | c >> 6 & 0x3F));
                        buffer.putByte((byte)(0x80 | c & 0x3F));
                    } else {
                        buffer.putByte((byte)(0xC0 | c >> 6 & 0x1F));
                        buffer.putByte((byte)(0x80 | c & 0x3F));
                    }
                }

            }
        } else {
            byte[] compressed = compression.compress(attribute);
            int length = compressed.length;
            assert length>0;
            VariableLong.writePositive(buffer,(((long)length)<<COMPRESSOR_BIT_LEN) + compression.getId());
            buffer.putBytes(compressed);
        }

    }

    private enum CompressionType {

        NO_COMPRESSION {

            @Override
            public byte[] compress(String text) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String decompress(ScanBuffer buffer, int numBytes) {
                throw new UnsupportedOperationException();
            }
        },

        GZIP {
            @Override
            public byte[] compress(String text) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    OutputStream out = new GZIPOutputStream(baos);
                    out.write(text.getBytes("UTF-8"));
                    out.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return baos.toByteArray();
            }

            @Override
            public String decompress(final ScanBuffer buffer, final int numBytes) {
                try {
                    InputStream in = new GZIPInputStream(new InputStream() {

                        int bytesRead = 0;

                        @Override
                        public int read() throws IOException {
                            if (++bytesRead>numBytes) return -1;
                            return 0xFF & buffer.getByte();
                        }
                    });
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] bytes = new byte[8192];
                    int len;
                    while ((len = in.read(bytes)) > 0)
                        baos.write(bytes, 0, len);
                    return new String(baos.toByteArray(), "UTF-8");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };


        public abstract byte[] compress(String text);

        public abstract String decompress(ScanBuffer buffer, int numBytes);

        public int getId() {
            return this.ordinal();
        }

        public static CompressionType getFromId(int id) {
            for (CompressionType ct : values()) if (ct.getId()==id) return ct;
            throw new IllegalArgumentException("Unknown compressor type for id: "+id);
        }

    }

}
