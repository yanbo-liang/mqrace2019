package io.openmessaging;


import java.nio.ByteBuffer;

public class CompressUtil {

    public static int compress(long[] uncompressed, ByteBuffer compressed, int start) {
        // 32 bit int, total bit length
        // 32 bit int, uncompressed length
        // 64 bit long, start value
        compressed.putInt(4 + start, uncompressed.length);
        compressed.putLong(8 + start, uncompressed[0]);
        long last = uncompressed[0];
        int pointer = 128;
        for (int i = 1; i < uncompressed.length; i++) {
            long current = uncompressed[i];
            if (last == current) {
                pointer++;
            } else if (last < current) {
                long diff = current - last;
                last = current;
                long count = 0;
                while (count < diff) {
                    flipBit(compressed, start, pointer++);
                    count++;
                }
                pointer++;
            }
        }
        compressed.putInt(start, pointer);
        return pointer / 8 + 1;
    }

    public static long[] decompress(ByteBuffer compressed, int start) {
        long[] uncompressed = new long[compressed.getInt(4 + start)];
        uncompressed[0] = compressed.getLong(8 + start);
        long last = uncompressed[0];
        int i = 1;
        for (int pointer = 128; pointer < compressed.getInt(start); pointer++) {
            if (increaseBit(compressed, start, pointer)) {
                last++;
                continue;
            }
            uncompressed[i++] = last;
        }
        return uncompressed;
    }


    private static void flipBit(ByteBuffer compressed, int start, int bitPointer) {
        int byteIndex = bitPointer / 8;
        int bitIndex = 7 - bitPointer % 8;
        byte b = compressed.get(byteIndex + start);
        b ^= 1 << bitIndex;
        compressed.put(byteIndex + start, b);
    }

    private static boolean increaseBit(ByteBuffer compressed, int start, int bitPointer) {
        int byteIndex = bitPointer / 8;
        int bitIndex = 7 - bitPointer % 8;
        int tmp = (compressed.get(byteIndex + start) >> bitIndex) & 1;
        return tmp == 1;
    }
}
