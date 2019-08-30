package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DirectBufferManager {
    private static BlockingQueue<ByteBuffer> messageBufferQueue = new LinkedBlockingQueue<>();
    private static BlockingQueue<ByteBuffer> headerBufferQueue = new LinkedBlockingQueue<>();
    private static ByteBuffer compressedBuffer;

    static {
        compressedBuffer = ByteBuffer.allocateDirect((int) Constants.Compressed_Buffer_Size);
        long queueSize = (Constants.Direct_Memory_Size - Constants.Compressed_Buffer_Size) / (Constants.Write_Body_Buffer_Size + Constants.Write_A_Buffer_Size);
        for (long i = 0; i < queueSize; i++) {
            messageBufferQueue.offer(ByteBuffer.allocateDirect((int) Constants.Write_Body_Buffer_Size));
            headerBufferQueue.offer(ByteBuffer.allocateDirect((int) Constants.Write_A_Buffer_Size));
        }
    }

    public static ByteBuffer getCompressedBuffer() {
        return compressedBuffer;
    }

    public static ByteBuffer borrowBuffer() throws Exception {
        ByteBuffer buffer = messageBufferQueue.take();
        buffer.clear();
        return buffer;
    }

    public static void returnBuffer(ByteBuffer buffer) throws Exception {
        messageBufferQueue.put(buffer);
    }

    public static ByteBuffer borrowHeaderBuffer() throws Exception {
        ByteBuffer buffer = headerBufferQueue.take();
        buffer.clear();
        return buffer;
    }

    public static void returnHeaderBuffer(ByteBuffer buffer) throws Exception {
        headerBufferQueue.put(buffer);
    }

    public static void changeToRead() {
//        List<ByteBuffer> tmp = new ArrayList<>();
//        messageBufferQueue.drainTo(tmp);
//        headerBufferQueue.drainTo(tmp);
//        for (ByteBuffer buffer : tmp) {
//            ((DirectBuffer) buffer).cleaner().clean();
//        }
//
//        long queueSize = (Constants.Direct_Memory_Size - Constants.Compressed_Buffer_Size) / Constants.Read_Buffer_Size;
//        for (long i = 0; i < queueSize; i++) {
//            ByteBuffer buffer = ByteBuffer.allocateDirect((int) Constants.Read_Buffer_Size);
//            messageBufferQueue.offer(buffer);
//        }
    }
}
