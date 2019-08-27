package io.openmessaging.unsafe;

public class UnsafeBuffer {
    private long bufferAddress;

    UnsafeBuffer(int bufferSize) {
        bufferAddress = UnsafeWrapper.getUnsafe().allocateMemory(bufferSize);
    }

    void putLong(long index, long l) {
        UnsafeWrapper.getUnsafe().putLong(bufferAddress + index, l);
    }

    void put(long index, byte[] data) {
        for (long i = 0; i < data.length; i++) {
            UnsafeWrapper.getUnsafe().putByte(bufferAddress + index + i, data[(int) i]);
        }
    }
}
