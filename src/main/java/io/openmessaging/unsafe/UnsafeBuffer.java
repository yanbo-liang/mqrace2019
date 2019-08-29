package io.openmessaging.unsafe;

import io.openmessaging.Constants;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class UnsafeBuffer {
    private ByteBuffer byteBuffer;
    private long address;
    private int limit;

    public UnsafeBuffer(int size) {
        try {
            byteBuffer = ByteBuffer.allocateDirect(size);
            Field addressField = Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            address = addressField.getLong(byteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
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

    public void position(int position) {
        byteBuffer.position(position);
    }

    public void flip() {
        byteBuffer.flip();
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
