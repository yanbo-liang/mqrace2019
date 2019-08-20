package io.openmessaging;

import java.nio.ByteBuffer;


public class CompressTest {

    public static void main(String[] args) {
        ByteBuffer compressed = ByteBuffer.allocateDirect(200 * 1024 * 1024);
        long s = System.currentTimeMillis();
        int start = 0;
        for (int j = 0; j < 3; j++) {
            long[] input = new long[200000];
            for (int i = 0; i < input.length; i++) {
                input[i] = i;
            }
            System.out.println(start);
            int compressedByte = CompressUtil.compress(input, compressed, start);
            start += compressedByte;
        }
        System.out.println(System.currentTimeMillis() - s);
        long s1 = System.currentTimeMillis();

        long[] decompress = CompressUtil.decompress(compressed, 100032);

        for (int i = 0; i < decompress.length; i++) {

            if (decompress[i] != i) {
                System.out.println("fucked");
            }
        }
        System.out.println(System.currentTimeMillis() - s1);
    }
}
