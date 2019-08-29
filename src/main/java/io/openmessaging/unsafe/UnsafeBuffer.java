package io.openmessaging.unsafe;

public class UnsafeBuffer {
    private long address;
    private int size;
    private int limit;

    public UnsafeBuffer(int size) {
        address = UnsafeWrapper.getUnsafe().allocateMemory(size);
        this.size = size;
    }

    long getAddress() {
        return address;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public long getLong(int index) {
        return UnsafeWrapper.getUnsafe().getLong(address + index);
    }

    public void putLong(int index, long l) {
        UnsafeWrapper.getUnsafe().putLong(address + index, l);
    }

    public void put(int index, byte[] data) {
        for (long i = 0; i < data.length; i++) {
            UnsafeWrapper.getUnsafe().putByte(address + index + i, data[(int) i]);
        }
    }

    public byte getByte(int index) {
        return UnsafeWrapper.getUnsafe().getByte(address + index);
    }

    public void free() {
        UnsafeWrapper.getUnsafe().freeMemory(address);
    }

    public static void copy(UnsafeBuffer buffer1, int start1, UnsafeBuffer buffer2, int start2, int length) {
        UnsafeWrapper.getUnsafe().copyMemory(buffer1.address + start1, buffer2.address + start2, length);
    }

    static void copy(long srcAddress, long destAddress, long length) {
        UnsafeWrapper.getUnsafe().copyMemory(srcAddress, destAddress, length);
    }
}
