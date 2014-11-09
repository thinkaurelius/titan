package com.thinkaurelius.titan.diskstorage.util;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public class VInt {
    public static int sizeof(long i) {
        if (i >= -112 && i <= 127)
            return 1;

        int size = 0;
        int len = -112;
        if (i < 0) {
            i ^= -1L; // take one's complement'
            len = -120;
        }
        long tmp = i;
        while (tmp != 0) {
            tmp = tmp >> 8;
            len--;
        }
        size++;
        len = (len < -120) ? -(len + 120) : -(len + 112);
        size += len;
        return size;
    }

    public static void encode(ByteBuf out, long i) {
        if (i >= -112 && i <= 127) {
            out.writeByte((byte) i);
            return;
        }
        int len = -112;
        if (i < 0) {
            i ^= -1L; // take one's complement'
            len = -120;
        }
        long tmp = i;
        while (tmp != 0) {
            tmp = tmp >> 8;
            len--;
        }

        out.writeByte((byte) len);
        len = (len < -120) ? -(len + 120) : -(len + 112);
        for (int idx = len; idx != 0; idx--) {
            int shiftbits = (idx - 1) * 8;
            long mask = 0xFFL << shiftbits;
            out.writeByte((byte) ((i & mask) >> shiftbits));
        }
    }

    public static long decode(ByteBuf in) {
        byte firstByte = in.readByte();
        int len = decodeSize(firstByte);
        if (len == 1)
            return firstByte;
        long i = 0;
        for (int idx = 0; idx < len - 1; idx++) {
            byte b = in.readByte();
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return (isNegative(firstByte) ? (i ^ -1L) : i);
    }

    private static int decodeSize(byte value) {
        if (value >= -112) {
            return 1;
        } else if (value < -120) {
            return -119 - value;
        }
        return -111 - value;
    }

    private static boolean isNegative(byte value) {
        return value < -120 || (value >= -112 && value < 0);
    }
}
