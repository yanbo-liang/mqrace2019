package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DirectBufferManager {
    private static BlockingQueue<ByteBuffer> bufferQueue = new LinkedBlockingQueue<>();
    private static BlockingQueue<ByteBuffer> smallBufferQueue = new LinkedBlockingQueue<>();

    static {

        long queueSize = Constants.Direct_Memory_Size / (Constants.Direct_Write_Buffer_Size + Constants.Direct_Write_Small_Buffer_Size);

        for (long i = 0; i < queueSize; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) Constants.Direct_Write_Buffer_Size);
            ByteBuffer smallBuffer = ByteBuffer.allocateDirect((int) Constants.Direct_Write_Small_Buffer_Size);
            bufferQueue.offer(buffer);
            smallBufferQueue.offer(smallBuffer);
        }
    }

    public static ByteBuffer borrowBuffer() {
        try {
            ByteBuffer buffer = bufferQueue.take();
            buffer.clear();
            return buffer;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void returnBuffer(ByteBuffer buffer) {
        try {
            bufferQueue.put(buffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ByteBuffer borrowSmallBuffer() {
        try {
            ByteBuffer buffer = smallBufferQueue.take();
            buffer.clear();
            return buffer;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void returnSmallBuffer(ByteBuffer buffer) {
        try {
            smallBufferQueue.put(buffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void changeToRead() {
        List<ByteBuffer> tmp = new ArrayList<>();
        bufferQueue.drainTo(tmp);
        for (ByteBuffer buffer : tmp) {
            ((DirectBuffer) buffer).cleaner().clean();
        }

        long queueSize = Constants.Direct_Memory_Size / Constants.Direct_Read_Buffer_Size;

        for (long i = 0; i < queueSize; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) Constants.Direct_Read_Buffer_Size);
            bufferQueue.offer(buffer);
        }
    }
}
