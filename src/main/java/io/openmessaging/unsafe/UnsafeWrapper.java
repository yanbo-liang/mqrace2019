package io.openmessaging.unsafe;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

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

    static Unsafe getUnsafe() {
        return unsafe;
    }
}
