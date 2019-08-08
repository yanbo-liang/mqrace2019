package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DirectBufferManager {
    private static BlockingQueue<ByteBuffer> bufferQueue = new LinkedBlockingQueue<>();

    static  {
        long queueSize = Constants.Direct_Memory_Size / Constants.Direct_Buffer_Size;

        for (long i = 0; i < queueSize; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) Constants.Direct_Buffer_Size);
            bufferQueue.offer(buffer);
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
}
