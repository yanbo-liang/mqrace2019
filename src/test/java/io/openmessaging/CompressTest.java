package io.openmessaging;

import java.nio.ByteBuffer;


public class CompressTest {

    public static void main(String[] args) {
        ByteBuffer compressed = ByteBuffer.allocateDirect(200 * 1024 * 1024);
        long s = System.currentTimeMillis();
        int start = 0;
        for (int j = 0; j < 3; j++) {
            ByteBuffer uncompressed = ByteBuffer.allocateDirect(200000 * 8);
            long l = 0;
            while (uncompressed.hasRemaining()) {
                uncompressed.putLong(l++);
            }
            uncompressed.flip();
            System.out.println(start);
            int compressedByte = CompressUtil.compress(uncompressed, compressed, start);
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
