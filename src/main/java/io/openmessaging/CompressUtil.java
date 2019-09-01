package io.openmessaging;


import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class CompressUtil {

    public static int compress(ByteBuffer uncompressed, ByteBuffer compressed, int start) {
        // 32 bit int, total bit length
        // 32 bit int, uncompressed length
        // 64 bit long, start value
        long first = uncompressed.getLong(0);
        compressed.putInt(4 + start, uncompressed.limit() / 8);
        compressed.putLong(8 + start, first);
        long last = first;
        int pointer = 128;
        for (int i = 8; i < uncompressed.limit(); i += 8) {
            long current = uncompressed.getLong(i);
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
        if (pointer % 8 == 0) {
            return pointer / 8;
        } else {
            return pointer / 8 + 1;
        }
    }

    public static void decompress(ByteBuffer compressed, LongBuffer uncompressed, int start) {
        long base = compressed.getLong(8 + start);
        uncompressed.put(base);
        long last = base;
        for (int pointer = 128; pointer < compressed.getInt(start); pointer++) {
            if (increaseBit(compressed, start, pointer)) {
                last++;
                continue;
            }
            uncompressed.put(last);
        }
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
