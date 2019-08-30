package io.openmessaging.unsafe;

import io.openmessaging.Constants;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class UnsafeWrapper {
    private static Unsafe unsafe;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Unsafe getUnsafe() {
        return unsafe;
    }

    public static void unsafeCopy(long[] src, long srcStart, ByteBuffer dest, long destStart, long length) {
        try {
            Field addressField = Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            long address = addressField.getLong(dest);
            unsafe.copyMemory(src, Unsafe.ARRAY_LONG_BASE_OFFSET + srcStart * Unsafe.ARRAY_LONG_INDEX_SCALE, null, address + destStart * Unsafe.ARRAY_LONG_INDEX_SCALE, length * 8);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void unsafeCopy(byte[] src, long srcStart, ByteBuffer dest, long destStart, long length) {
        try {
            Field addressField = Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            long address = addressField.getLong(dest);
            unsafe.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET + srcStart * Constants.Body_Size, null, address + destStart * Constants.Body_Size, length * Constants.Body_Size);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
