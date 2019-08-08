package io.openmessaging;

public class MemoryTest {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        byte[] array = new byte[10 * 1024 * 1024];


        for (int i = 0; i < array.length; i++) {
            byte b = array[i];

        }

        long end = System.currentTimeMillis();

        System.out.println(end - start);
    }

}

