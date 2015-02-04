package com.thinkaurelius.titan.diskstorage.mapdb;

import java.io.Serializable;


/**
 * Encodes database key and data items as a byte array.
 * Based on BerekelyDB's DatabaseEntry class
 *
 * <p>Storage and retrieval methods are based on key/data
 * pairs. Both key and data items are represented by DBEntry objects.
 * Key and data byte arrays may refer to arrays of zero length up to arrays of
 * essentially unlimited length.</p>
 *
 * <p>The DatabaseEntry class provides simple access to an underlying object
 * whose elements can be examined or changed.  DatabaseEntry objects can be
 * subclassed, providing a way to associate with it additional data or
 * references to other structures.</p>
 *
 * <p>Access to DatabaseEntry objects is not re-entrant. In particular, if
 * multiple threads simultaneously access the same DatabaseEntry object using
 *
 * <p>Also note that for DatabaseEntry output parameters, the method called
 * will always allocate a new byte array.  The byte array specified by the
 * caller will not be used.  Therefore, after calling a method that returns
 * output parameters, the application can safely keep a reference to the byte
 * array returned by {@link #getData} without danger that the array will be
 * overwritten in a subsequent call.</p>
 *
 * <h3>Offset and Size Properties</h3>
 *
 * <p>By default the Offset property is zero and the Size property is the
 * length of the byte array.  However, to allow for optimizations involving the
 * partial use of a byte array, the Offset and Size may be set to non-default
 * values.</p>
 *
 * <p>For DatabaseEntry output parameters, the Size will always be set to the
 * length of the byte array and the Offset will always be set to zero.</p>
 *
 * <p>However, for DatabaseEntry input parameters the Offset and Size are set
 * to non-default values by the built-in tuple and serial bindings.  For
 * example, with a tuple or serial binding the byte array is grown dynamically
 * as data is output, and the Size is set to the number of bytes actually used.
 * For a serial binding, the Offset is set to a non-zero value in order to
 * implement an optimization having to do with the serialization stream
 * header.</p>
 *
 * <p>Therefore, for output DatabaseEntry parameters the application can assume
 * that the Offset is zero and the Size is the length of the byte
 * array. However, for input DatabaseEntry parameters the application should
 * not make this assumption.  In general, it is safest for the application to
 * always honor the Size and Offset properties, rather than assuming they have
 * default values.</p>
 *
 * <p>By default the specified data (byte array, offset and size) corresponds
 * to the full stored key or data item.
 *
 */
public class DBEntry implements Serializable , Comparable<DBEntry>{
    private static final long serialVersionUID = 1L;

    /* Currently, JE stores all data records as byte array */
    private byte[] data;
    private int offset = 0;
    private int size = 0;


    /* FindBugs - ignore not "final" since a user can set this. */
    /** @hidden
     * The maximum number of bytes to show when toString() is called.
     */
    public static int MAX_DUMP_BYTES = 100;

    /**
     * Returns all the attributes of the database entry in text form, including
     * the underlying data.  The maximum number of bytes that will be formatted
     * is taken from the static variable DatabaseEntry.MAX_DUMP_BYTES, which
     * defaults to 100.  MAX_DUMP_BYTES may be changed by an application if it
     * wishes to cause more bytes to be formatted.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("<DatabaseEntry");
        sb.append(" offset=\"").append(offset);
        sb.append("\" size=\"").append(size);
        sb.append("\" data=\"").append(dumpData());
        if ((size - 1) > MAX_DUMP_BYTES) {
            sb.append(" ... ").append((size - MAX_DUMP_BYTES) +
                    " bytes not shown ");
        }
        sb.append("\"/>");
        return sb.toString();
    }

    /**
     * Copies up to MAX_DUMP_BYTES to a byte array
     * @return
     */
    private byte[] dumpData() {
        if (this.data.length<= this.MAX_DUMP_BYTES)
            return data;
        else
        {
            byte[] res = new byte[MAX_DUMP_BYTES];
            for (int i =0 ; i<MAX_DUMP_BYTES ; i++)
                res[i] = data[i];
            return res ;
        }

    }

    /*
     * Constructors
     */

    /**
     * Constructs a DatabaseEntry with a given byte array.  The offset is set
     * to zero; the size is set to the length of the array, or to zero if null
     * is passed.
     *
     * @param data Byte array wrapped by the DatabaseEntry.
     */
    public DBEntry(byte[] data) {
        this.data = data;
        if (data != null) {
            this.size = data.length;
        }
    }

    /**
     * Constructs a DatabaseEntry with a given byte array, offset and size.
     *
     * @param data Byte array wrapped by the DatabaseEntry.
     *
     * @param offset Offset in the first byte in the byte array to be included.
     *
     * @param size Number of bytes in the byte array to be included.
     */
    public DBEntry(byte[] data, int offset, int size) {
        this.data = data;
        this.offset = offset;
        this.size = size;
    }

    /*
     * Accessors
     */

    /**
     * Returns the byte array.
     *
     * <p>For a DatabaseEntry that is used as an output parameter, the byte
     * array will always be a newly allocated array.  The byte array specified
     * by the caller will not be used and may be null.</p>
     *
     * @return The byte array.
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Sets the byte array.  The offset is set to zero; the size is set to the
     * length of the array, or to zero if null is passed.
     *
     * @param data Byte array wrapped by the DatabaseEntry.
     */
    public void setData(byte[] data) {
        this.data = data;
        offset = 0;
        size = (data == null) ? 0 : data.length;
    }

    /**
     * Sets the byte array, offset and size.
     *
     * @param data Byte array wrapped by the DatabaseEntry.
     *
     * @param offset Offset in the first byte in the byte array to be included.
     *
     * @param size Number of bytes in the byte array to be included.
     */
    public void setData(byte[] data, int offset, int size) {
        this.data = data;
        this.offset = offset;
        this.size = size;
    }

    /**
     * Returns the byte offset into the data array.
     *
     * <p>For a DatabaseEntry that is used as an output parameter, the offset
     * will always be zero.</p>
     *
     * @return Offset in the first byte in the byte array to be included.
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Sets the byte offset into the data array.
     *
     * ArrayIndexOutOfBoundsException if the data, offset, and size parameters
     * refer to elements of the data array which do not exist.  Note that this
     * exception will not be thrown by setSize() or setOffset(), but will be
     * thrown by varous JE methods if "this" is inconsistent and is used as an
     * input parameter to those methods.  It is the caller's responsibility to
     * ensure that size, offset, and data.length are consistent.
     *
     * @param offset Offset in the first byte in the byte array to be included.
     */
    public void setOffset(int offset) {
        this.offset = offset;
    }

    /**
     * Returns the byte size of the data array.
     *
     * <p>For a DatabaseEntry that is used as an output parameter, the size
     * will always be the length of the data array.</p>
     *
     * @return Number of bytes in the byte array to be included.
     */
    public int getSize() {
        return size;
    }

    /**
     * Sets the byte size of the data array.
     *
     * ArrayIndexOutOfBoundsException if the data, offset, and size parameters
     * refer to elements of the data array which do not exist.  Note that this
     * exception will not be thrown by setSize() or setOffset(), but will be
     * thrown by varous JE methods if "this" is inconsistent and is used as an
     * input parameter to those methods.  It is the caller's responsibility to
     * ensure that size, offset, and data.length are consistent.
     *
     * @param size Number of bytes in the byte array to be included.
     */
    public void setSize(int size) {
        this.size = size;
    }


    /**
     * Compares the data of two entries for byte-by-byte equality.
     *
     * <p>In either entry, if the offset is non-zero or the size is not equal
     * to the data array length, then only the data bounded by these values is
     * compared.  The data array length and offset need not be the same in both
     * entries for them to be considered equal.</p>
     *
     * <p>If the data array is null in one entry, then to be considered equal
     * both entries must have a null data array.</p>
     *
     * <p>If the partial property is set in either entry, then to be considered
     * equal both entries must have the same partial properties: partial,
     * partialOffset and partialLength.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DBEntry)) {
            return false;
        }
        DBEntry e = (DBEntry) o;
        if (data == null && e.data == null)
            return true;

        if (data == null || e.data == null)
            return false;

        if (size != e.size)
            return false;

        for (int i = 0; i < size; i += 1)
            if (data[offset + i] != e.data[e.offset + i])
                return false;

        return true;
    }

    /**
     * Returns a hash code based on the data value.
     */
    @Override
    public int hashCode() {
        int hash = 0;
        if (data != null) {
            for (int i = 0; i < size; i += 1) {
                hash += data[offset + i];
            }
        }
        return hash;
    }

    @Override
    public int compareTo(DBEntry o) {
        for (int i = 0; i < this.data.length && i < o.getData().length; i++)
            if (this.data[i] != o.getData()[i])
                return (this.data[i] & 0xFF) - (o.getData()[i] & 0xFF);
        return this.data.length - o.getData().length;
    }
}

