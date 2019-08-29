package io.openmessaging;

import io.openmessaging.unsafe.UnsafeBuffer;
import io.openmessaging.unsafe.UnsafeWriter;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrayPut {


    private static ConcurrentHashMap<Long, BufferWrapper> map = new ConcurrentHashMap<>();

    public static void put(Message message) throws Exception {
        BufferWrapper bufferWrapper = map.get(message.getT() / 1000 * 1000);
        if (bufferWrapper == null) {
            synchronized (ArrayPut.class) {
                bufferWrapper = map.get(message.getT() / 1000 * 1000);
                if (bufferWrapper == null) {
                    long s = System.currentTimeMillis();
                    bufferWrapper = BufferWarpperManager.borrow();
                    map.put(message.getT() / 1000 * 1000, bufferWrapper);
                }
            }
        }
        ByteBuffer buffer = bufferWrapper.buffer;
        buffer.putLong(message.getT());
         buffer.putLong(message.getA());
         buffer.put(message.getBody());
    }

    private static class BufferWrapper {
        ByteBuffer buffer;
        long min;

        public BufferWrapper(long min) {
            buffer = ByteBuffer.allocate(100 * 1000 * Constants.Message_Size);
            this.min = min;
        }
    }

    private static class BufferWarpperManager {
        private static Queue<BufferWrapper> unused = new ConcurrentLinkedQueue<>();
        private static Queue<BufferWrapper> used = new ConcurrentLinkedQueue<>();

        static {
            for (int i = 0; i < 200; i++) {
                unused.offer(new BufferWrapper(0));
            }
        }

        static BufferWrapper borrow() {
            if (unused.peek() == null) {
                unused.offer(used.poll());
            }
            BufferWrapper poll = unused.poll();
            used.offer(poll);
            poll.buffer.clear();
            return poll;
        }
    }
}