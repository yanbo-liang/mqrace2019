package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DirectBufferManager {
    private static BlockingQueue<ByteBuffer> bodyBufferQueue = new LinkedBlockingQueue<>();
    private static BlockingQueue<ByteBuffer> aBufferQueue = new LinkedBlockingQueue<>();
    private static ByteBuffer compressedBuffer;

    static {
        compressedBuffer = ByteBuffer.allocateDirect((int) Constants.Compressed_Buffer_Size);
        long queueSize = (Constants.Direct_Memory_Size - Constants.Compressed_Buffer_Size) / (Constants.Write_Body_Buffer_Size + Constants.Write_A_Buffer_Size);
        System.out.println("queueSize" + queueSize);
        for (long i = 0; i < queueSize; i++) {
            bodyBufferQueue.offer(ByteBuffer.allocateDirect((int) Constants.Write_Body_Buffer_Size));
            aBufferQueue.offer(ByteBuffer.allocateDirect((int) Constants.Write_A_Buffer_Size));
        }
    }

    public static ByteBuffer getCompressedBuffer() {
        return compressedBuffer;
    }

    public static ByteBuffer borrowBodyBuffer() throws Exception {
        ByteBuffer buffer = bodyBufferQueue.take();
        buffer.clear();
        return buffer;
    }

    public static void returnBodyBuffer(ByteBuffer buffer) throws Exception {
        bodyBufferQueue.put(buffer);
    }

    public static ByteBuffer borrowABuffer() throws Exception {
        ByteBuffer buffer = aBufferQueue.take();
        buffer.clear();
        return buffer;
    }

    public static void returnABuffer(ByteBuffer buffer) throws Exception {
        aBufferQueue.put(buffer);
    }

    public static void freeWriteBuffer() {
        List<ByteBuffer> tmp = new ArrayList<>();
        bodyBufferQueue.drainTo(tmp);
        aBufferQueue.drainTo(tmp);
        for (ByteBuffer buffer : tmp) {
            ((DirectBuffer) buffer).cleaner().clean();
        }
    }
}
