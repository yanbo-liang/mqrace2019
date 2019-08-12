package io.openmessaging;

import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class CompressUtil {
    private static Deflater compresser = new Deflater(5);
    private static Inflater uncompresser = new Inflater();

    public static int compress(byte[] uncompressed, int uStart, int uLength, byte[] compressed, int cStart, int cLength) {
        compresser.reset();
        compresser.setInput(uncompressed, uStart, uLength);
        compresser.finish();
        return compresser.deflate(compressed, cStart, cLength);
    }
}
