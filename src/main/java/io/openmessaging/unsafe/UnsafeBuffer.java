package io.openmessaging.unsafe;

public class UnsafeBuffer {
    private long bufferAddress;
    private long bufferSize;

    UnsafeBuffer(int bufferSize) {
        bufferAddress = UnsafeWrapper.getUnsafe().allocateMemory(bufferSize);
        this.bufferSize = bufferSize;
    }

    long getLong(int index) {
        return UnsafeWrapper.getUnsafe().getLong(bufferAddress + index);
    }

    void putLong(long index, long l) {
        UnsafeWrapper.getUnsafe().putLong(bufferAddress + index, l);
    }

    void put(long index, byte[] data) {
        for (long i = 0; i < data.length; i++) {
            UnsafeWrapper.getUnsafe().putByte(bufferAddress + index + i, data[(int) i]);
        }
    }

    void free() {
        UnsafeWrapper.getUnsafe().freeMemory(bufferAddress);
    }

    static void copy(UnsafeBuffer buffer1, UnsafeBuffer buffer2) {
        if (buffer1.bufferSize != buffer2.bufferSize) {
            throw new RuntimeException("unequal buffer size");
        }
        UnsafeWrapper.getUnsafe().copyMemory(buffer1.bufferAddress, buffer2.bufferAddress, buffer1.bufferSize);
    }
}
