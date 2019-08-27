package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

public class DirectBufferManager {
    private static BlockingQueue<ByteBuffer> messageBufferQueue = new LinkedBlockingQueue<>();
    private static BlockingQueue<ByteBuffer> headerBufferQueue = new LinkedBlockingQueue<>();
    private static ByteBuffer compressedBuffer;

    static {
        long queueSize = Constants.Direct_Memory_Size / Constants.Message_Write_Buffer_Size;
        for (long i = 0; i < queueSize; i++) {
            ByteBuffer messageBuffer = ByteBuffer.allocateDirect((int) Constants.Message_Write_Buffer_Size);
            messageBufferQueue.offer(messageBuffer);

        }
//        long queueSize = (Constants.Direct_Memory_Size - Constants.Compressed_Buffer_Size) / (Constants.Message_Write_Buffer_Size + Constants.Header_Write_Buffer_Size);
//        compressedBuffer = ByteBuffer.allocateDirect((int) Constants.Compressed_Buffer_Size);
//
//        for (long i = 0; i < queueSize; i++) {
//            ByteBuffer messageBuffer = ByteBuffer.allocateDirect((int) Constants.Message_Write_Buffer_Size);
//            messageBufferQueue.offer(messageBuffer);
//            ByteBuffer headerBuffer = ByteBuffer.allocateDirect((int) Constants.Header_Write_Buffer_Size);
//            headerBufferQueue.offer(headerBuffer);
//        }
    }

    public static ByteBuffer getCompressedBuffer() {
        return compressedBuffer;
    }

    public static ByteBuffer borrowBuffer() {
        try {
            ByteBuffer buffer = messageBufferQueue.take();
            buffer.clear();
            return buffer;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void returnBuffer(ByteBuffer buffer) {
        try {
            messageBufferQueue.put(buffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ByteBuffer borrowHeaderBuffer() {
        try {
            ByteBuffer buffer = headerBufferQueue.take();
            buffer.clear();
            return buffer;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void returnHeaderBuffer(ByteBuffer buffer) {
        try {
            headerBufferQueue.put(buffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void changeToRead() {
        List<ByteBuffer> tmp = new ArrayList<>();
        messageBufferQueue.drainTo(tmp);
        headerBufferQueue.drainTo(tmp);
        for (ByteBuffer buffer : tmp) {
            ((DirectBuffer) buffer).cleaner().clean();
        }

        long queueSize = (Constants.Direct_Memory_Size - Constants.Compressed_Buffer_Size) / Constants.Direct_Read_Buffer_Size;

        for (long i = 0; i < queueSize; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) Constants.Direct_Read_Buffer_Size);
            messageBufferQueue.offer(buffer);
        }
    }
}
