package io.openmessaging;

import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class ZipTest {
    public static void main(String[] args) {
        int size = 100000;
        byte[] data = new byte[size * 8];
        byte[] output = new byte[size * 8];
        for (int k = 0; k < size; k++) {
            ByteUtils.putLong(data, k, (k * 8));
        }
        Deflater deflater = new Deflater(1);
        deflater.setInput(data);
        deflater.finish();

        long start = System.currentTimeMillis();
        int compressedSize = deflater.deflate(output);
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(data.length + "->" + compressedSize);
        // Decompress the bytes
        Inflater decompresser = new Inflater();
         start = System.currentTimeMillis();

        decompresser.setInput(output, 0, compressedSize);

        try {
            int resultLength = decompresser.inflate(data);
            System.out.println(System.currentTimeMillis() - start);

        } catch (Exception e) {
            e.printStackTrace();
        }
        decompresser.end();
    }
//        try {
//            byte[] a = new byte[600000*8];
//            for (int k = 0; k < 600000; k++) {
//                ByteUtils.putLong(a, k, (k * 8));
//            }
//
//
//            long start = System.currentTimeMillis();
//
//            LZ4Factory factory = LZ4Factory.unsafeInstance();
//
//            final int decompressedLength = a.length;
//
//// compress data
//            LZ4Compressor compressor = factory.highCompressor(1);
//            int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
//            byte[] compressed = new byte[maxCompressedLength];
//            int compressedLength = compressor.compress(a, 0, decompressedLength, compressed, 0, maxCompressedLength);
//            System.out.println(compressedLength +" "+ decompressedLength);
//            System.out.println(decompressedLength);
//
//            start = System.currentTimeMillis();
//
//// decompress data
//// - method 1: when the decompressed length is known
//            LZ4FastDecompressor decompressor = factory.fastDecompressor();
//            byte[] restored = new byte[decompressedLength];
//            int compressedLength2 = decompressor.decompress(compressed, 0, restored, 0, decompressedLength);
//            System.out.println(System.currentTimeMillis() - start);
//
//        } catch (
//                Exception e) {
//            e.printStackTrace();
//        }
// compressedLength == compressedLength2
//        int[] data = new int[5000000];
//        IntegratedIntCompressor iic = new IntegratedIntCompressor();
//
//
//        System.out.println("Compressing "+data.length+" integers using friendly interface");
//        int[] compressed = iic.compress(data);
//        long start = System.currentTimeMillis();
//
//        int[] recov = iic.uncompress(compressed);
//        System.out.println("compressed from "+data.length*4/1024+"KB to "+compressed.length*4/1024+"KB");
//        System.out.println(System.currentTimeMillis()-start);
//    }

//    }
}