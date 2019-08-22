package io.openmessaging;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

public class LongBufferTest {

    public static void main(String[] args) {

        long a = 6666666;
        long[] longArray = new long[1];
        ByteBuffer source = ByteBuffer.allocate(8);
        source.putLong(a);
        source.flip();
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        LongArrayUtils.byteArrayToLongArray(longArray, 0, source.array());
        LongArrayUtils.longArraytoByteBuffer(longArray, 0, byteBuffer);
        byteBuffer.flip();
        System.out.println(longArray[0]);
        System.out.println(byteBuffer.getLong());

        System.out.println(Arrays.toString(byteBuffer.array()));
        if (byteBuffer.getLong() != a) {
            System.out.println("fuck");
        }


    }
}

